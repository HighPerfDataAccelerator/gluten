#!/usr/bin/env python3
"""Upper bound on what removing BSP stage barriers could save, from a Spark event log.
usage: barrier_bound.py <eventlog> <queries.json> [slots]"""
import json, sys, bisect
log, qj = sys.argv[1], sys.argv[2]
SLOTS = int(sys.argv[3]) if len(sys.argv) > 3 else 336
queries = json.load(open(qj))
tasks = []   # (launch, finish)
jobs = {}    # id -> [start,end]
with open(log, errors='replace') as f:
    for line in f:
        if '"SparkListenerTaskEnd"' in line:
            e = json.loads(line); ti = e['Task Info']
            if ti.get('Failed') or ti.get('Killed'): continue
            if ti['Finish Time'] > 0: tasks.append((ti['Launch Time'], ti['Finish Time']))
        elif '"SparkListenerJobStart"' in line:
            e = json.loads(line); jobs[e['Job ID']] = [e['Submission Time'], None]
        elif '"SparkListenerJobEnd"' in line:
            e = json.loads(line)
            if e['Job ID'] in jobs: jobs[e['Job ID']][1] = e['Completion Time']
tasks.sort()
starts = [t[0] for t in tasks]
jl = sorted((s, e) for s, e in jobs.values() if e)
rows = []
for q in queries:
    if q['status'] != 'success': continue
    s, e = q['start'] * 1000, q['end'] * 1000
    # job-active union clipped to the query window
    act, cur_s, cur_e = 0.0, None, None
    for js, je in jl:
        if je < s or js > e: continue
        js, je = max(js, s), min(je, e)
        if cur_e is None or js > cur_e:
            if cur_e is not None: act += cur_e - cur_s
            cur_s, cur_e = js, je
        else: cur_e = max(cur_e, je)
    if cur_e is not None: act += cur_e - cur_s
    i = bisect.bisect_left(starts, s - 1)
    busy, ntask = 0.0, 0
    # sweep for time with <50% slots busy inside active windows (ramp/tail)
    ev = []
    for l, fi in tasks[i:]:
        if l > e: break
        l2, f2 = max(l, s), min(fi, e)
        if f2 > l2: ev += [(l2, 1), (f2, -1)]; busy += f2 - l2; ntask += 1
    ev.sort()
    wall = e - s
    rows.append(dict(query=q['query'], wall=wall / 1000, gap=(wall - act) / 1000, active=act / 1000,
                     busy_slot_s=busy / 1000, tasks=ntask,
                     bound=(wall - act) / 1000 + busy / 1000 / SLOTS))
tot = lambda k: sum(r[k] for r in rows)
W, G, B = tot('wall'), tot('gap'), sum(r['bound'] for r in rows)
print(json.dumps(dict(log=log, slots=SLOTS, wall=W, driver_gap=G, busy_over_slots=tot('busy_slot_s') / SLOTS,
                      bound_total=B, max_saving_frac=1 - B / W, queries=len(rows)), indent=1))
rows.sort(key=lambda r: -r['wall'])
for r in rows[:12]:
    print('%-10s wall %6.2f gap %5.2f busy/slots %6.2f bound %6.2f tasks %d' % (r['query'], r['wall'], r['gap'], r['busy_slot_s'] / SLOTS, r['bound'], r['tasks']))
json.dump(rows, open(log.split('/')[-1] + '.bound.json', 'w'))
