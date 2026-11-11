import pathlib,json,subprocess,sys,os,time
r=pathlib.Path(__file__).resolve().parent;suite,variant,label=sys.argv[1:4];unit=r.name+'-'+label+'.scope';cg=pathlib.Path('/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice')/unit
config=json.loads(pathlib.Path(sys.argv[sys.argv.index('--conf-json')+1]).read_text());expected_executors=int(config['spark.executor.instances']);assert expected_executors == 42
assert not cg.exists();statepath=r/(label+'-cgroup-status.json');assert not statepath.exists();env=dict(os.environ,XDG_RUNTIME_DIR='/run/user/1000')
cmd=['systemd-run','--user','--scope','--unit='+unit,'-p','MemorySwapMax=0','--',sys.executable,str(r/'inside.py'),*sys.argv[1:]]
(r/(label+'-cgroup-command.json')).write_text(json.dumps(cmd,indent=2));page=os.sysconf('SC_PAGE_SIZE');assert page==65536
vm=lambda:dict(l.split() for l in pathlib.Path('/proc/vmstat').read_text().splitlines());initial=vm();seen={};p=None;samples=0;driver_offset=0;driver_tail=""
def swapbytes():
 now=vm();return (int(now['pswpin'])+int(now['pswpout'])-int(initial['pswpin'])-int(initial['pswpout']))*page
try:
 statepath.write_text(json.dumps(dict(status='running',unit=unit)))
 p=subprocess.Popen(cmd,env=env,cwd=r)
 with (r/(label+'-cgroup-resources.jsonl')).open('w') as trace:
  while p.poll() is None:
   time.sleep(.5);swap=swapbytes();snap=dict(time=time.time(),host_swap_io_bytes=swap)
   # Cgroup OOM counters do not detect Java heap OOM. Check incremental driver output.
   driver_log=r/'results'/f'{label}-{suite}-{variant}'/'driver.log'
   if driver_log.exists():
    with driver_log.open(errors='replace') as guard_log:
     guard_log.seek(driver_offset);new_text=driver_tail+guard_log.read();driver_offset=guard_log.tell()
    if 'OutOfMemoryError' in new_text:
     hit=new_text.index('OutOfMemoryError');evidence=dict(utc=time.time(),driver_log=str(driver_log),context=new_text[max(0,hit-300):hit+2500],reason='Java heap OOM violatesOOM0;stop scope immediately,do not wait for Spark retries/deadline')
     (r/(label+'-jvm-oom-guard.json')).write_text(json.dumps(evidence,indent=2))
     raise RuntimeError('Java OutOfMemoryError detected; campaign stopped')
    driver_tail=new_text[-200:]

   if cg.exists():
    try:
     vals={n:(cg/n).read_text().strip() for n in ['memory.swap.max','memory.swap.current','memory.events','memory.current','memory.peak','memory.pressure']};snap['cgroup']=vals
     assert vals['memory.swap.max']=='0' and vals['memory.swap.current']=='0';ev=dict(l.split() for l in vals['memory.events'].splitlines());assert int(ev['oom'])==int(ev['oom_kill'])==0
     procs=[]
     for proc in pathlib.Path('/proc').iterdir():
      if not proc.name.isdigit():continue
      try:
       args=(proc/'cmdline').read_bytes()
       if not any(n in args for n in [b'org.apache.spark.deploy.master.Master',b'org.apache.spark.deploy.worker.Worker',b'org.apache.spark.deploy.SparkSubmit',b'CoarseGrainedExecutorBackend']):continue
       member=(proc/'cgroup').read_text().strip().split('::',1)[1];assert member=='/'+str(cg.relative_to('/sys/fs/cgroup')),(proc.name,member)
       status={l.split(':')[0]:l.split(':',1)[1].strip() for l in (proc/'status').read_text().splitlines() if ':' in l};row=dict(pid=int(proc.name),cgroup=member,executor=b'CoarseGrainedExecutorBackend' in args,VmRSS=status.get('VmRSS'),VmSwap=status.get('VmSwap'),cpus=status.get('Cpus_allowed_list'));procs.append(row);seen[proc.name]=row
      except (FileNotFoundError,ProcessLookupError):continue
     snap['spark_processes']=procs
    except FileNotFoundError:pass
   trace.write(json.dumps(snap)+'\n');trace.flush();samples+=1
   assert swap<=16*2**20,('Whole-hostswapIO exceeds16MiB',swap)
 assert p.returncode==0,p.returncode
 swap=swapbytes();assert swap<=16*2**20
 final=json.loads((r/(label+'-cgroup-final.json')).read_text());assert final['returncode']==0
 assert len([x for x in seen.values() if x['executor']])==expected_executors
 d=r/'results'/f'{label}-{suite}-{variant}';launch=json.loads((d/'launch.json').read_text());assert launch['performance_eligible']==(not label.startswith('smoke') and '--profile' not in sys.argv) and launch['swap_activity_bytes']<=16*2**20
 statepath.write_text(json.dumps(dict(status='passed',unit=unit,samples=samples,executor_membership_verified=expected_executors,host_swap_io_bytes=swap,group_final=final),indent=2))
except BaseException as e:
 statepath.write_text(json.dumps(dict(status='failed',error=repr(e),unit=unit),indent=2))
 if p is not None and p.poll() is None:subprocess.run(['systemctl','--user','stop',unit],env=env,check=False);p.wait()
 launchpath=r/'results'/f'{label}-{suite}-{variant}'/'launch.json'
 if launchpath.exists():
  launch=json.loads(launchpath.read_text());launch['performance_eligible']=False;launch['timing_invalid_reason']='Cgroup/resource guard: '+repr(e);launchpath.write_text(json.dumps(launch,indent=2))
 raise
finally:(r/(label+'-process-membership.json')).write_text(json.dumps(seen,indent=2))
