#!/usr/bin/env python3
"""Caller chains for samples whose stack contains a marker. usage: jfr_callers.py <jfr bin> <rec> <thread-prefix> <marker>..."""
import json, re, subprocess, sys
from collections import Counter
jfr, rec, tp, *marks = sys.argv[1:]
ev = json.loads(subprocess.run([jfr, 'print', '--json', '--stack-depth', '200', '--events', 'jdk.ExecutionSample', rec],
                               capture_output=True, text=True, check=True).stdout)['recording']['events']
ev = [e for e in ev if ((e['values'].get('sampledThread') or {}).get('javaName') or '').startswith(tp)]
for m in marks:
    c = Counter()
    for e in ev:
        fs = [re.sub(r'\$\$Lambda[^.]*', '', f['method']['type']['name']).split('.')[-1] + '.' + f['method']['name']
              + ':' + str(f.get('lineNumber', '')) for f in e['values']['stackTrace']['frames']]
        idx = next((i for i, f in enumerate(fs) if m in f), None)
        if idx is None: continue
        chain = [f for f in fs[idx + 1:] if f.startswith(('org/apache', 'org.apache'))][:7]
        c[' < '.join(chain)] += 1
    print(f'== {m}: {sum(c.values())} samples')
    for k, v in c.most_common(5): print(f'  {v:4d}  {k}')
