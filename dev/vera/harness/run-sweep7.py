"""Screening sweep: one NDS app per arm, strictly serial. conf = candidate.json + overrides. Run from this dir."""
from pathlib import Path
import pathlib
import json,os,subprocess,sys,shutil,time
r=Path(__file__).resolve().parent
base=json.loads((r/"candidate.json").read_text())
full=json.loads(Path("/home/nvidia/ferd/benchmarks/vera-stack-20261106/full.json").read_text())
NDS=[x for x in full if x["suite"]=="tpcds"][0]["queries"]
NDSH=[x for x in full if x["suite"]=="tpch"][0]["queries"]
BC=("spark.sql.autoBroadcastJoinThreshold","spark.sql.adaptive.autoBroadcastJoinThreshold")
P="spark.gluten.sql.columnar."
V=P+"backend.velox."
ARMS=[
 ("p200",{"spark.sql.shuffle.partitions":"200"}),
]
if __name__=="__main__":
  suite=sys.argv[1]; arms=[ARMS[0]]
  if len(sys.argv)>2: arms=[a for a in arms if a[0] in sys.argv[2].split(",")]
  qs=NDS if suite=="tpcds" else NDSH
  for i,(name,ov) in enumerate(arms):
    label=("pg%02d"%i+suite[-2:])+name
    conf=dict(base);conf.update(ov);cf="conf-%s.json"%name;(r/cf).write_text(json.dumps(conf,indent=1))
    spec=[dict(label=label,suite=suite,artifact="pgo",arm=name,conf=cf,queries=qs,warmup=False,resource_slots=336)]
    sf=r/("sched-%s.json"%label);sf.write_text(json.dumps(spec))
    busy=[]
    for p in Path("/proc").iterdir():
      if not p.name.isdigit() or int(p.name)==os.getpid():continue
      try:
        args=(p/"cmdline").read_bytes().split(b"\0")
        if args[0].endswith(b"java") and any(b"SparkSubmit" in a or b"CoarseGrainedExecutorBackend" in a or b"deploy.master" in a or b"deploy.worker" in a for a in args):busy.append(p.name)
      except OSError:pass
    mem={l.split(":")[0]:int(l.split()[1])*1024 for l in Path("/proc/meminfo").read_text().splitlines()}
    t=time.time();ok=False;err=None
    if busy or mem["MemAvailable"]<12*2**30: err="preflight busy=%s avail=%.1f"%(busy,mem["MemAvailable"]/2**30)
    else:
      env=dict(os.environ,XDG_RUNTIME_DIR="/run/user/1000")
      rc=subprocess.run([sys.executable,str(r/"run-monitored-strict.py"),str(sf)],cwd=r,env=env).returncode
      ok=rc==0; err=None if ok else "rc=%d"%rc
    ev=r/"results"/(label+"-"+suite+"-candidate")/"runtime"/"events"
    dst=pathlib.Path("/data/nvme1/rtm/eventlogs")/label; dst.mkdir(parents=True,exist_ok=True)
    [shutil.move(str(f),str(dst/f.name)) for f in ev.iterdir()] if ev.exists() else None
    with (r/("sweep7-%s.log"%suite)).open("a") as f: f.write(json.dumps(dict(label=label,arm=name,ok=ok,err=err,wall=time.time()-t))+"\n")
    time.sleep(10)
