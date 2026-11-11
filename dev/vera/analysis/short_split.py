#!/usr/bin/env python3
"""Split idle slot-time while tasks run and nothing is pending into:
  cap  : the task count of all running stages is below the slot count (parallelism limit of the DAG)
  tail : the running stages have >= slots tasks in total, the idle slots are stragglers' tails
Also histogram stage task counts weighted by stage wall time. usage: short_split.py <log> <queries.json>"""
import json, sys
from collections import defaultdict
SLOTS = 336
log, qpath = sys.argv[1], sys.argv[2]
queries = [q for q in json.load(open(qpath)) if q['status'] == 'success']
win = [(q['start'] * 1000, q['end'] * 1000) for q in queries]
tasks, sub, done = [], {}, {}
names = {}
with open(log, errors='replace') as f:
    for line in f:
        if line.startswith('{"Event":"SparkListenerTaskEnd"'):
            e = json.loads(line); ti = e['Task Info']
            if not ti.get('Failed') and ti['Finish Time'] > 0:
                tasks.append((ti['Launch Time'], ti['Finish Time'], (e['Stage ID'], e['Stage Attempt ID'])))
        elif line.startswith('{"Event":"SparkListenerStageSubmitted"'):
            si = json.loads(line)['Stage Info']
            k = (si['Stage ID'], si['Stage Attempt ID']); sub[k] = (si.get('Submission Time'), si['Number of Tasks']); names[k] = si['Stage Name'][:40]
        elif line.startswith('{"Event":"SparkListenerStageCompleted"'):
            si = json.loads(line)['Stage Info']; done[(si['Stage ID'], si['Stage Attempt ID'])] = si.get('Completion Time')
first = defaultdict(lambda: 1e18); last = defaultdict(float)
for a, b, k in tasks:
    first[k] = min(first[k], a); last[k] = max(last[k], b)
ev = []
for a, b, k in tasks:
    ev += [(a, 1, 0, k), (b, -1, 0, k)]
for k, (s, n) in sub.items():
    if s is None: continue
    ev += [(s, 0, 1, k), ((done.get(k) or last[k]), 0, -1, k)]
ev.sort(key=lambda x: x[0])
R = 0; active = defaultdict(int); launched = defaultdict(int)
acc = defaultdict(float); prev = None
def inq(t): return any(a <= t < b for a, b in win)
for t, dr, ds, k in ev:
    if prev is not None and t > prev and inq(prev):
        dt = t - prev
        pending = sum(sub[s][1] - launched[s] for s in active if active[s] > 0)
        if R > 0 and pending <= 0 and R < SLOTS:
            ntasks = sum(sub[s][1] for s in active if active[s] > 0)
            acc['cap' if ntasks < SLOTS else 'tail'] += (SLOTS - R) * dt
            acc['cap_slots_lost_if_all_ran'] += max(0, SLOTS - ntasks) * dt if ntasks < SLOTS else 0
    if dr == 1: R += 1; launched[k] += 1
    elif dr == -1: R -= 1
    if ds: active[k] += ds
    prev = t
T = sum((b - a) for a, b in win) * SLOTS
for k in ('cap', 'tail'):
    print(f'{k:5s} {100 * acc[k] / T:5.1f}% of slot-time')
print(f'  of cap: slots no running stage could ever use (sum of stage tasks < slots): {100 * acc["cap_slots_lost_if_all_ran"] / T:5.1f}%')
hist = defaultdict(float)
for k, (s, n) in sub.items():
    if k in first:
        w = last[k] - first[k]
        b = '1' if n == 1 else '2-63' if n < 64 else '64-199' if n < 200 else '200' if n == 200 else '201-335' if n < SLOTS else '>=336'
        hist[b] += w
tot = sum(hist.values())
print('stage wall time by task count:', {b: f'{100 * v / tot:.0f}%' for b, v in sorted(hist.items())})
