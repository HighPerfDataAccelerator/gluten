#!/usr/bin/env python3
"""Decompose 'no job active' wall time inside each query window (the driver-gap share of idle slots).

usage: driver_gaps.py <eventlog> <queries.json> [out.json]

Per query window [qs, qe] (harness timestamps: spark.sql(...) + collect):
  pre_sql     qs -> first SQLExecutionStart   (parse/analyze/optimize/physical plan incl. Gluten
                                               transform+validation of the initial plan)
  sql_to_job  first SQLExecutionStart -> first JobStart (AQE initial stage creation, subqueries)
  inter_job   no job active between first JobStart and last JobEnd (AQE re-optimisation, new stage
              planning/Gluten transform, broadcast relation build on driver, subquery eval)
  post        last JobEnd -> qe (result fetch/decode, SQLExecutionEnd, python side)
Each inter_job gap is tagged with the kind of job that follows it.
"""
import json
import re
import sys
from collections import defaultdict

log, qpath = sys.argv[1], sys.argv[2]
OUT = sys.argv[3] if len(sys.argv) > 3 else None
queries = [q for q in json.load(open(qpath)) if q['status'] == 'success']

jobs = {}        # id -> [start, end, desc]
sql_start = []   # (time, execId)
aqe_updates = [] # approx time (last timestamp seen)
last_t = 0
SKIP = ('{"Event":"SparkListenerTaskEnd"', '{"Event":"SparkListenerTaskStart"',
        '{"Event":"SparkListenerExecutorMetricsUpdate"', '{"Event":"SparkListenerStageExecutorMetrics"',
        '{"Event":"SparkListenerBlockManagerAdded"', '{"Event":"SparkListenerTaskGettingResult"')
with open(log, errors='replace') as f:
    for line in f:
        if line.startswith(SKIP):
            continue
        if line.startswith('{"Event":"SparkListenerJobStart"'):
            e = json.loads(line)
            p = e.get('Properties') or {}
            desc = p.get('spark.job.description') or p.get('callSite.short') or ''
            jobs[e['Job ID']] = [e['Submission Time'], None, desc,
                                 len(e.get('Stage Infos') or []), p.get('spark.sql.execution.id')]
            last_t = e['Submission Time']
        elif line.startswith('{"Event":"SparkListenerJobEnd"'):
            e = json.loads(line)
            if e['Job ID'] in jobs:
                jobs[e['Job ID']][1] = e['Completion Time']
            last_t = e['Completion Time']
        elif 'SparkListenerSQLExecutionStart' in line[:120]:
            e = json.loads(line)
            sql_start.append((e['time'], e['executionId']))
            last_t = e['time']
        elif 'SparkListenerSQLAdaptiveExecutionUpdate' in line[:120]:
            aqe_updates.append(last_t)


def kind(desc):
    d = desc.lower()
    if 'broadcast' in d:
        return 'broadcast'
    if 'subquery' in d:
        return 'subquery'
    if re.search(r'\bcollect\b|executecollect|head at|take at|tolocaliterator', d):
        return 'collect'
    return desc.split(' at ')[0][:40] or 'unknown'


rows, by_kind = [], defaultdict(lambda: [0.0, 0])
for q in queries:
    qs, qe = q['start'] * 1000, q['end'] * 1000
    js = sorted((s, e or qe, d) for s, e, d, _n, _x in jobs.values() if qs - 1 <= s <= qe + 1)
    ss = [t for t, _ in sql_start if qs - 1 <= t <= qe + 1]
    r = dict(query=q['query'], wall=(qe - qs) / 1000, jobs=len(js),
             aqe_updates=sum(1 for t in aqe_updates if qs <= t <= qe))
    if not js:
        r.update(pre_sql=0, sql_to_job=0, inter_job=0, post=0, nojob_total=(qe - qs) / 1000)
        rows.append(r)
        continue
    first_sql = min(ss) if ss else js[0][0]
    r['pre_sql'] = max(0, first_sql - qs) / 1000
    r['sql_to_job'] = max(0, js[0][0] - first_sql) / 1000
    inter, cover_end, ngap = 0.0, js[0][1], 0
    for s, e, d in js[1:]:
        if s > cover_end:
            g = (s - cover_end) / 1000
            inter += g
            ngap += 1
            by_kind[kind(d)][0] += g
            by_kind[kind(d)][1] += 1
        cover_end = max(cover_end, e)
    r['inter_job'] = inter
    r['gaps'] = ngap
    r['post'] = max(0, qe - cover_end) / 1000
    r['nojob_total'] = r['pre_sql'] + r['sql_to_job'] + inter + r['post']
    rows.append(r)

W = sum(r['wall'] for r in rows)
print(f'{len(rows)} queries, wall {W:.1f} s, jobs {sum(r["jobs"] for r in rows)}, '
      f'AQE plan updates {sum(r["aqe_updates"] for r in rows)}')
for k in ['pre_sql', 'sql_to_job', 'inter_job', 'post', 'nojob_total']:
    v = sum(r.get(k, 0) for r in rows)
    print(f'  {k:12s} {v:7.2f} s  {100 * v / W:5.1f}% of wall')
print('inter-job gaps by following job kind (s, count):')
for k, (v, n) in sorted(by_kind.items(), key=lambda x: -x[1][0])[:10]:
    print(f'  {k:40s} {v:7.2f} s  n={n}  avg {1000 * v / n:6.1f} ms')
print('top queries by no-job time:')
for r in sorted(rows, key=lambda r: -r['nojob_total'])[:12]:
    print(f"  {r['query']:12s} wall {r['wall']:6.2f}  nojob {r['nojob_total']:5.2f}  pre {r['pre_sql']:4.2f}"
          f"  s2j {r['sql_to_job']:4.2f}  inter {r['inter_job']:4.2f} ({r.get('gaps', 0)} gaps)"
          f"  post {r['post']:4.2f}  jobs {r['jobs']}")
if OUT:
    json.dump({'rows': rows, 'by_kind': by_kind}, open(OUT, 'w'), indent=1)
