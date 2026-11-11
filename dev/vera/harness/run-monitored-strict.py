import hashlib
from audit_common import audit
import concurrent.futures
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time

r = Path(__file__).resolve().parent
schedule = json.loads(Path(sys.argv[1]).read_text())
tag = Path(sys.argv[1]).stem
state_path = r / (tag + '-status.json')
assert not state_path.exists()
state = {'status': 'running', 'runs': []}
hz = os.sysconf('SC_CLK_TCK')

def save():
    state_path.write_text(json.dumps(state, indent=2))

def proc_cpu(pid):
    s = Path(f'/proc/{pid}/stat').read_text()
    a = s[s.rfind(')')+2:].split()
    return (int(a[11]) + int(a[12])) / hz

def disks():
    out={}
    for line in Path('/proc/diskstats').read_text().splitlines():
        a=line.split()
        if re.fullmatch(r'nvme\d+n\d+',a[2]):
            out[a[2]]={'read_bytes':int(a[5])*512,'write_bytes':int(a[9])*512,'io_ms':int(a[12]),'weighted_io_ms':int(a[13])}
    return out

def host_cpu():
    return list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))

try:
    for spec in schedule:
        name = f"{spec['label']}-{spec['suite']}-candidate"
        d = r / 'results' / name
        state['active'] = name
        save()
        cmd = [sys.executable, str(r/'run-suite-strict.py'), spec['suite'], 'candidate',
               spec['label'], '--conf-json', str(r/spec['conf'])]
        cmd += ["--artifact", spec.get("artifact", "control")]
        if spec.get('queries'):
            cmd += ['--queries', spec['queries']]
        if spec.get('warmup'):
            cmd += ['--warmup']
        with (r/'logs'/(name+'.log')).open('w') as log:
            process = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
            prev_t = time.time()
            prev_host = host_cpu()
            prev_disks = disks()
            prev = {}
            known = {}
            samples_path = r/(name+'-cpu.jsonl')
            with samples_path.open('w') as f:
                while process.poll() is None:
                    time.sleep(.5)
                    now = time.time()
                    dt = now-prev_t
                    preflight = d/'executor-native-preflight.json'
                    if preflight.exists() and not known:
                        try:
                            known = {x['pid']: 'executor' for x in json.loads(preflight.read_text())}
                            for p in Path('/proc').iterdir():
                                if not p.name.isdigit(): continue
                                try: c = (p/'cmdline').read_bytes()
                                except OSError: continue
                                if str(d).encode() in c and b'org.apache.spark.deploy.SparkSubmit' in c and b'java' in c.split(b'\0')[0]:
                                    known[int(p.name)] = 'driver'
                        except (OSError, ValueError):
                            known = {}
                    current = {}
                    roles = {'executor': 0., 'driver': 0.}
                    for pid, role in known.items():
                        try: current[pid] = proc_cpu(pid)
                        except OSError: continue
                        if pid in prev: roles[role] += (current[pid]-prev[pid])/dt
                    h = host_cpu()
                    diff = [a-b for a,b in zip(h,prev_host)]
                    total = sum(diff)
                    row = {'start':prev_t,'end':now,'executor_cores':roles['executor'],
                           'driver_cores':roles['driver'],
                           'host_busy_percent':100*sum(diff[i] for i in [0,1,2,5,6])/total,
                           'host_iowait_percent':100*diff[4]/total}
                    cur_disks=disks()
                    row['disks']={name:{k:(v-prev_disks[name][k])/dt for k,v in vals.items()} for name,vals in cur_disks.items() if name in prev_disks}
                    prev_disks=cur_disks
                    f.write(json.dumps(row)+'\n')
                    f.flush()
                    prev_t, prev_host, prev = now, h, current
        assert process.returncode == 0, (name, process.returncode)
        assert json.loads((d/'correctness.json').read_text())['equal']
        assert json.loads((d/'launch.json').read_text())['performance_eligible'], 'Timing excluded: inspect launch.json swap activity' 
        qs = json.loads((d/'queries.json').read_text())
        audited = audit(spec['label'],spec['suite'],spec['queries'].split(','),spec['conf'],False,int(spec.get('warmup',False)),spec['artifact'])
        activation = json.loads((d/'stateless-activation.json').read_text())
        assert activation['enabled'] == (json.loads((r/spec['conf']).read_text())['spark.gluten.experimental.statelessValidation.enabled'] == 'true')
        if activation['enabled']: assert activation['native_validation_calls'] > 0 and activation['equivalence_checks'] == 0
        effective = json.loads((d/'effective-conf.json').read_text())
        assert 'qualification.jar' not in effective['spark.driver.extraClassPath'] and '-javaagent:' not in effective['spark.driver.extraJavaOptions']
        (r/(spec['label']+'-audit.json')).write_text(json.dumps(dict(audit=audited,activation=activation),indent=2))
        samples = [json.loads(x) for x in samples_path.read_text().splitlines()]
        for q in qs:
            weighted = [(x,max(0,min(q['end'],x['end'])-max(q['start'],x['start']))) for x in samples]
            covered = sum(w for x,w in weighted)
            assert covered >= (q['end']-q['start'])*.98
            for k in ['executor_cores','driver_cores','host_busy_percent','host_iowait_percent']:
                q[k] = sum(x[k]*w for x,w in weighted)/covered
        record = dict(spec, name=name, queries=qs, summary=json.loads((d/'summary.json').read_text()), correct=True)
        state['runs'].append(record)
        save()
        print('DONE', name, [(q['query'],round(q['seconds'],3),round(q['executor_cores'],1)) for q in qs], flush=True)
    state.update(status='completed',active=None)
except BaseException as exc:
    state.update(status='failed',error=repr(exc))
    raise
finally:
    save()
