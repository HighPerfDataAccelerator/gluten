#!/usr/bin/env python3
"""Where do the task slots go? Decompose slot-time of every query from a Spark event log.

usage: sched_breakdown.py <eventlog> <queries.json> [slots] [out.json]

For each query window, sweeping time:
  R(t) running tasks, P(t) launchable-but-not-launched tasks of submitted stages, J(t) jobs active.
Idle slots (slots - R) are attributed, in order, to:
  sched_delay   : P > 0, i.e. runnable tasks exist but were not launched yet (min(P, idle))
  no_job        : no job active (driver: planning, AQE re-optimisation, broadcast/subquery waits
                  outside a job, result collection)
  stage_gap     : a job is active, no task running and nothing pending (between stages)
  short_stage   : a job is active, tasks running, nothing pending: stage tails and stages with
                  fewer tasks than slots (barrier + parallelism shortfall)
Busy slots (R) are split with per-task metrics into
  cpu           : Executor CPU Time (task thread, includes JNI/native work on that thread)
  shuffle_wait  : shuffle fetch wait time
  other_wait    : remaining run time (I/O, native helper threads, blocking, GC not in CPU)
  overhead      : deserialize + result serialization + launch/finish bookkeeping
"""
import json
import sys
from collections import defaultdict

log, qpath = sys.argv[1], sys.argv[2]
SLOTS = int(sys.argv[3]) if len(sys.argv) > 3 else 336
OUT = sys.argv[4] if len(sys.argv) > 4 else None

queries = [q for q in json.load(open(qpath)) if q['status'] == 'success']
tasks = []          # dict(launch, finish, stage, cpu, fetch_wait, run, deser, rser)
stage_submit = {}   # (stage, attempt) -> (submit_ms, num_tasks)
stage_done = {}
jobs = {}
with open(log, errors='replace') as f:
    for line in f:
        if line.startswith('{"Event":"SparkListenerTaskEnd"'):
            e = json.loads(line)
            ti, tm = e['Task Info'], e.get('Task Metrics') or {}
            if ti.get('Failed') or ti.get('Killed') or ti['Finish Time'] <= 0:
                continue
            sr = tm.get('Shuffle Read Metrics') or {}
            tasks.append(dict(
                launch=ti['Launch Time'], finish=ti['Finish Time'],
                stage=(e['Stage ID'], e['Stage Attempt ID']),
                cpu=tm.get('Executor CPU Time', 0) / 1e6, run=tm.get('Executor Run Time', 0),
                fetch_wait=sr.get('Fetch Wait Time', 0),
                deser=tm.get('Executor Deserialize Time', 0),
                rser=tm.get('Result Serialization Time', 0)))
        elif line.startswith('{"Event":"SparkListenerStageSubmitted"'):
            si = json.loads(line)['Stage Info']
            if si.get('Submission Time'):
                stage_submit[(si['Stage ID'], si['Stage Attempt ID'])] = (si['Submission Time'], si['Number of Tasks'])
        elif line.startswith('{"Event":"SparkListenerStageCompleted"'):
            si = json.loads(line)['Stage Info']
            stage_done[(si['Stage ID'], si['Stage Attempt ID'])] = si.get('Completion Time')
        elif line.startswith('{"Event":"SparkListenerJobStart"'):
            e = json.loads(line)
            jobs[e['Job ID']] = [e['Submission Time'], None]
        elif line.startswith('{"Event":"SparkListenerJobEnd"'):
            e = json.loads(line)
            if e['Job ID'] in jobs:
                jobs[e['Job ID']][1] = e['Completion Time']

launches = defaultdict(list)
for t in tasks:
    launches[t['stage']].append(t['launch'])
for v in launches.values():
    v.sort()


def sweep(qs, qe):
    """Integrate the categories over [qs, qe] (ms). Returns slot-ms per category."""
    ev = []   # (time, kind, delta)
    for t in tasks:
        a, b = max(t['launch'], qs), min(t['finish'], qe)
        if b > a:
            ev += [(a, 'R', 1), (b, 'R', -1)]
    for key, (sub, n) in stage_submit.items():
        done = stage_done.get(key) or qe
        a, b = max(sub, qs), min(done, qe)
        if b <= a:
            continue
        # pending = n - launched(t); emit +n at submit, -1 per launch (first attempts only)
        ev.append((a, 'P', n - sum(1 for x in launches[key] if x <= a)))
        for x in launches[key]:
            if a < x < b:
                ev.append((x, 'P', -1))
        ev.append((b, 'P', -(n - sum(1 for x in launches[key] if x < b))))
    for js, je in jobs.values():
        a, b = max(js, qs), min(je or qe, qe)
        if b > a:
            ev += [(a, 'J', 1), (b, 'J', -1)]
    ev.sort()
    acc = defaultdict(float)
    R = P = J = 0
    prev = qs
    for time, kind, d in ev + [(qe, 'end', 0)]:
        dt = time - prev
        if dt > 0:
            idle = max(SLOTS - R, 0)
            acc['busy'] += min(R, SLOTS) * dt
            sd = min(max(P, 0), idle)
            acc['sched_delay'] += sd * dt
            rest = idle - sd
            if J <= 0:
                acc['no_job'] += rest * dt
            elif R == 0 and P <= 0:
                acc['stage_gap'] += rest * dt
            else:
                acc['short_stage'] += rest * dt
            prev = time
        if kind == 'R':
            R += d
        elif kind == 'P':
            P += d
        elif kind == 'J':
            J += d
    return acc


rows = []
for q in queries:
    qs, qe = q['start'] * 1000, q['end'] * 1000
    acc = sweep(qs, qe)
    inq = [t for t in tasks if t['launch'] >= qs - 1 and t['finish'] <= qe + 1]
    dur = sum(t['finish'] - t['launch'] for t in inq)
    cpu = sum(min(t['cpu'], t['finish'] - t['launch']) for t in inq)
    fw = sum(t['fetch_wait'] for t in inq)
    ovh = sum(t['deser'] + t['rser'] + max(0, (t['finish'] - t['launch']) - t['run'] - t['deser'] - t['rser']) for t in inq)
    total = (qe - qs) * SLOTS
    busy_split = dict(cpu=cpu, shuffle_wait=fw, overhead=ovh, other_wait=max(0.0, dur - cpu - fw - ovh))
    stages = {t['stage'] for t in inq}
    rows.append(dict(query=q['query'], wall_s=(qe - qs) / 1000, total_slot_s=total / 1000,
                     **{k: v / 1000 for k, v in acc.items()},
                     **{'busy_' + k: v / 1000 for k, v in busy_split.items()},
                     tasks=len(inq), stages=len(stages)))


def summarize(sel, label):
    T = sum(r['total_slot_s'] for r in sel)
    keys = ['no_job', 'stage_gap', 'short_stage', 'sched_delay', 'busy',
            'busy_cpu', 'busy_shuffle_wait', 'busy_other_wait', 'busy_overhead']
    s = {k: sum(r.get(k, 0) for r in sel) for k in keys}
    print(f'{label}: {len(sel)} queries, wall {sum(r["wall_s"] for r in sel):.1f} s, slot-time {T:.0f} slot-s')
    for k in keys:
        print(f'   {k:18s} {100 * s[k] / T:5.1f}%')
    return {k: s[k] / T for k in keys}


print(f'slots={SLOTS}  log={log}')
res = {'all': summarize(rows, 'ALL')}
res['short'] = summarize([r for r in rows if r['wall_s'] < 3], 'queries <3 s')
res['long'] = summarize([r for r in rows if r['wall_s'] >= 3], 'queries >=3 s')
print('\nlargest no_job+stage_gap+short_stage (slot-s):')
for r in sorted(rows, key=lambda r: -(r.get('no_job', 0) + r.get('stage_gap', 0) + r.get('short_stage', 0)))[:10]:
    print(f"  {r['query']:14s} wall {r['wall_s']:6.2f}  no_job {r.get('no_job', 0):7.0f}  gap {r.get('stage_gap', 0):6.0f}"
          f"  short {r.get('short_stage', 0):7.0f}  sched {r.get('sched_delay', 0):6.0f}  busy {r.get('busy', 0):7.0f}"
          f"  cpu {r['busy_cpu']:7.0f}  stages {r['stages']}  tasks {r['tasks']}")
if OUT:
    json.dump({'summary': res, 'rows': rows}, open(OUT, 'w'), indent=1)
