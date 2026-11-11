#!/usr/bin/env python3
"""Inclusive breakdown of one driver thread's JFR samples. usage: jfr_thread.py <jfr bin> <rec> <thread-prefix>"""
import json, re, subprocess, sys
from collections import Counter
jfr, rec, tp = sys.argv[1:4]
ev = json.loads(subprocess.run([jfr, 'print', '--json', '--stack-depth', '200', '--events', 'jdk.ExecutionSample', rec],
                               capture_output=True, text=True, check=True).stdout)['recording']['events']
ev = [e for e in ev if ((e['values'].get('sampledThread') or {}).get('javaName') or '').startswith(tp)]
def fr(e):
    out = []
    for f in e['values']['stackTrace']['frames']:
        t = re.sub(r'\$\$Lambda.*', '$$Lambda', f['method']['type']['name'])
        out.append(t + '.' + f['method']['name'])
    return out
n = len(ev); print(f'{n} samples on {tp}* (~{n*0.02:.1f} s CPU)')
incl = Counter(); cls = Counter(); leaf = Counter()
for e in ev:
    fs = fr(e)
    seen = set()
    for f in fs:
        c = f.rsplit('.', 1)[0]
        if c.startswith(('org.apache.spark.sql', 'org.apache.gluten', 'org.apache.spark.scheduler', 'org.apache.spark.rdd', 'org.apache.spark.broadcast')) and c not in seen:
            seen.add(c); cls[c] += 1
    leaf[next((f for f in fs if f.startswith(('org.apache', 'scala', 'java.util.zip', 'io.substrait', 'com.google'))), fs[0] if fs else '?')] += 1
print('\ninclusive by class (top 40):')
for k, v in cls.most_common(40): print(f'  {v:5d} {100*v/n:5.1f}%  {k}')
print('\nleaf-most library frame (top 20):')
for k, v in leaf.most_common(20): print(f'  {v:5d} {100*v/n:5.1f}%  {k}')
