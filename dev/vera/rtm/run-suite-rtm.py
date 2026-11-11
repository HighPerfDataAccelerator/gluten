#!/usr/bin/env python3
"""Isolated, NUMA-bound Spark 4 benchmark. Never overwrites a prior run."""
import argparse, datetime, hashlib, json, os, pathlib, resource, shutil
import signal, socket, subprocess, time, urllib.request
R = pathlib.Path('/home/nvidia/ferd')
E = pathlib.Path('/data/nvme1/rtm/exp')
ARTIFACTS = E / 'artifacts'
OLD = R / 'benchmarks/tpc-sf1000-20261028'
ALIGN = R / 'benchmarks/baseline-alignment-20261029'
p = argparse.ArgumentParser()
p.add_argument('suite', choices=['tpch','tpcds'])
p.add_argument('variant', choices=['parent','baseline','optimized','candidate','parent_candidate','partition_prefetch','null_prefetch','join128'])
p.add_argument('label')
p.add_argument('--queries', default='')
p.add_argument('--profile', action='store_true')
p.add_argument('--warmup', action='store_true')
p.add_argument('--conf-json')
p.add_argument("--artifact", choices=["control","pgo","gen","pgolto"], default="control")
a = p.parse_args()
if a.profile:
    probe = subprocess.run(['perf','stat','-e','cycles:u','--','true'], capture_output=True, text=True)
    if probe.returncode:
        raise RuntimeError('perf preflight failed: '+probe.stderr)
assert a.label.replace('-','').replace('_','').isalnum()
out = E / 'results' / f'{a.label}-{a.suite}-{a.variant}'
out.mkdir(parents=True, exist_ok=False)
resource.setrlimit(resource.RLIMIT_CORE, (0,0))
runtime = out / 'runtime'
for name in ['conf','events','tmp','warehouse','worker0','worker1']:
    (runtime/name).mkdir(parents=True)
jar = E/'artifact'/'gluten.jar'
assert a.conf_json  # one JAR for both arms; arms differ only by conf
selected_conf = json.loads(pathlib.Path(a.conf_json).read_text())
packed_setting = selected_conf['spark.executorEnv.VELOX_EXPERIMENTAL_PACKED_GROUP_KEYS']
assert packed_setting == '0'
package = json.loads(jar.with_name('package.json').read_text())
assert hashlib.sha256(jar.read_bytes()).hexdigest() == package['jar_sha256']
reference = json.loads((ALIGN/'nds-31-best-experiment.json').read_text())
historical = json.loads(reference['experiment']['p_config_json'])
conf = dict(historical['spark_configs'])
master_port, ui_port = 29077, 29080
conf.update({
    'spark.master':f'spark://127.0.0.1:{master_port}',
    'spark.app.name':out.name, 'spark.ui.enabled':'false',
    'spark.driver.memory':'16g', 'spark.cores.max':'336', 'spark.executor.instances':'42', 'spark.executor.memory':'6g',
    'spark.memory.offHeap.size':'22g', 'spark.executor.memoryOverhead':'6g',
    'spark.driver.extraClassPath':str(jar),'spark.executor.extraClassPath':str(jar),
    'spark.jars':str(jar), 'spark.eventLog.dir':(runtime/'events').as_uri(),
    'spark.eventLog.compress':'false','spark.eventLog.rolling.enabled':'false',
    'spark.sql.warehouse.dir':(runtime/'warehouse').as_uri(),
    'spark.cleaner.periodicGC.interval':'10s', 'spark.sql.session.timeZone':'UTC', 'spark.sql.debug.maxToStringFields':'10000',
    'spark.scheduler.minRegisteredResourcesRatio':'1.0',
    'spark.scheduler.maxRegisteredResourcesWaitingTime':'120s',
    'spark.driver.extraJavaOptions':f'-Xms1g -XX:G1PeriodicGCInterval=10000 -XX:MinHeapFreeRatio=20 -XX:MaxHeapFreeRatio=40 -XX:+ExplicitGCInvokesConcurrent -Dio.netty.tryReflectionSetAccessible=true -Djava.io.tmpdir={runtime}/tmp',
    'spark.executor.extraJavaOptions':f'-Xms1g -XX:G1PeriodicGCInterval=10000 -XX:MinHeapFreeRatio=20 -XX:MaxHeapFreeRatio=40 -XX:+ExplicitGCInvokesConcurrent -Dio.netty.tryReflectionSetAccessible=true -Djava.io.tmpdir={runtime}/tmp',
})
# Identical branch switches, including both SVE decoder switches, in every variant.
conf.update(json.loads((ALIGN/'historical-branch-switches.json').read_text()))
# Retain the existing optimization build's validated aggregation and partition settings.
conf.update({'spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemory':'1073741824',
    'spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemory':'1610612736',
    'spark.default.parallelism':'248', 'spark.sql.files.minPartitionNum':'248',
    'spark.sql.adaptive.coalescePartitions.minPartitionNum':'248'})
if a.conf_json:
    overrides=json.loads(pathlib.Path(a.conf_json).read_text())
    assert all(k.startswith('spark.') for k in overrides)
    conf.update({k:str(v) for k,v in overrides.items()})
shuffle = {0:[],1:[]}
for node, disks in [(0,[1,6]),(1,[2,3])]:
    for disk in disks:
        d=pathlib.Path(f'/data/nvme{disk}/ferd-vera-rtm-20261107')/out.name/f'node{node}'
        assert shutil.disk_usage(d.parent.parent if d.parent.parent.exists() else pathlib.Path(f'/data/nvme{disk}')).free > 512*1024**3
        d.mkdir(parents=True,exist_ok=False)
        shuffle[node].append(str(d))
conf['spark.local.dir']=','.join(shuffle[0])
conf_path=runtime/'conf/spark-defaults.conf'
conf_path.write_text('\n'.join(k+' '+v for k,v in sorted(conf.items()))+'\n')
(out/'requested-conf.json').write_text(json.dumps(conf,indent=2))
(out/'historical-conf-diff.json').write_text(json.dumps({k:{'historical':historical['spark_configs'].get(k),'current':v} for k,v in conf.items() if historical['spark_configs'].get(k)!=v},indent=2))
env=os.environ.copy()
for k in ['SPARK_SUBMIT_OPTS','SPARK_DRIVER_MEMORY','JAVA_TOOL_OPTIONS','_JAVA_OPTIONS','JDK_JAVA_OPTIONS','LD_PRELOAD']:
    env.pop(k,None)
spark=pathlib.Path('/data/nvme1/rtm/spark-5.0.0-rtm')
env.update({'JAVA_HOME':str(R/'.deps/root/usr/lib/jvm/java-17-openjdk-arm64'),
    'SPARK_HOME':str(spark),'SPARK_CONF_DIR':str(runtime/'conf'),
    'SPARK_LOCAL_IP':'127.0.0.1','PYSPARK_PYTHON':str(R/'.deps/spark4-env/bin/python'),
    'SPARK_DAEMON_MEMORY':'1g','TPC_NATIVE_PACKAGE':str(jar.with_name('package.json')),
    'TPC_DATA_ROOTS':str(ALIGN/'views'/a.suite/'sf1000'),'TPC_CACHE_POLICY':'retain',
    'TPC_PERF_PROFILE':'1' if a.profile else '0', 'TPC_WARMUP':'1' if a.warmup else '0', 'TPC_QUERIES':a.queries, 'TPC_QUERY_REPETITIONS':'1', 'TPC_EXECUTOR_COUNT':conf['spark.executor.instances'], 'TPC_EXECUTOR_CORES':conf['spark.executor.cores'], 'LD_PRELOAD':'/usr/lib/aarch64-linux-gnu/libjemalloc.so.2','MALLOC_CONF':'background_thread:true,narenas:8,dirty_decay_ms:1000,muzzy_decay_ms:1000'})
env['VELOX_EXPERIMENTAL_PACKED_GROUP_KEYS'] = packed_setting
executor_cores = int(conf['spark.executor.cores'])
executor_count = int(conf['spark.executor.instances'])
assert executor_cores == 8 and executor_count == 42
assert int(conf['spark.cores.max']) == executor_count * executor_cores
counts = (15, 16) if executor_count == 31 else (20, 22)
workers=[(0,'8-87,184-263',counts[0]*8,counts[0]*34),
         (1,'88-175,264-351',counts[1]*8,counts[1]*34)]
for port in [master_port,ui_port,29078,29079,29081,29082]:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        s.bind(('127.0.0.1',port))
children=[]; logs=[]
def launch(args,label,child_env):
    f=(out/(label+'.log')).open('w');logs.append(f)
    proc=subprocess.Popen(args,env=child_env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
    children.append(proc);return proc

def cluster():
    return json.load(urllib.request.urlopen(f'http://127.0.0.1:{ui_port}/json/',timeout=3))
def meminfo():
    return {line.split(':')[0]:int(line.split()[1])*1024 for line in pathlib.Path('/proc/meminfo').read_text().splitlines()}
start=time.time(); initial_mem=meminfo();initial_swap=initial_mem['SwapTotal']-initial_mem['SwapFree']
metadata={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'variant':a.variant,'suite':a.suite,
  'spark_runtime':'5.0.0-SNAPSHOT','spark_build':'5.0.0-SNAPSHOT','scala_build':'2.13.18','scala_runtime':'2.13.18','package':package,
  'worker_specs':workers,'shuffle_directories':shuffle,'cache_policy':'retain','warmups':int(a.profile or a.warmup),
  'historical_reference_id':761851325385267944, 'historical_resource_reference_id':761851325385267944,
  'limitations':[f'Host has 504 GiB physical RAM; worker memory is scheduling allowance, not physical RAM. Per-executor configured limits total {executor_count*34} GiB; actual allocation is guarded.',
    f'{executor_count}x{executor_cores} executor slots; worker affinity spans 336 logical CPUs; driver/master reserve 16 logical CPUs.',
    'Four NUMA-local shuffle disks, six input disks. Hardware capacity/layout differs from historical host.',
    'jemalloc narenas8 decay1s; periodic G1/cleaner GC10s and Xms1g retained from verified local harness; differs from historical JVM/allocator.',
    'Historical AB is an external reference, not a same-machine control; warmup exclusion is recorded separately.'],
  'memory_before':initial_mem,'selected_queries':a.queries, 'allocator_environment':{'LD_PRELOAD':'/usr/lib/aarch64-linux-gnu/libjemalloc.so.2','MALLOC_CONF':'background_thread:true,narenas:8,dirty_decay_ms:1000,muzzy_decay_ms:1000'},'jvm_reclamation_flags':'-Xms1g -XX:G1PeriodicGCInterval=10000 -XX:MinHeapFreeRatio=20 -XX:MaxHeapFreeRatio=40 -XX:+ExplicitGCInvokesConcurrent ', 'profiled':a.profile, 'performance_eligible':not a.profile and not a.label.startswith('smoke'), 'resource_profile':{'executors':executor_count,'cores_per_executor':executor_cores,'slots':executor_count*executor_cores,'numa_executor_counts':counts}, 'monitor_guards':{'minimum_host_available_GiB':12,'maximum_executor_swap_GiB':1}}
(out/'launch.json').write_text(json.dumps(metadata,indent=2))
try:
    launch(['numactl','--physcpubind=176-183','--membind=0',str(spark/'bin/spark-class'),'org.apache.spark.deploy.master.Master','--host','127.0.0.1','--port',str(master_port),'--webui-port',str(ui_port)],'master',env)
    for node,cpus,cores,memory in workers:
        we=dict(env,SPARK_LOCAL_DIRS=','.join(shuffle[node]))
        launch(['numactl','--physcpubind='+cpus,'--membind='+str(node),str(spark/'bin/spark-class'),'org.apache.spark.deploy.worker.Worker','--host','127.0.0.1','--port',str(29078+node),'--webui-port',str(29081+node),'--cores',str(cores),'--memory',str(memory)+'G','--work-dir',str(runtime/f'worker{node}'),f'spark://127.0.0.1:{master_port}'],f'worker{node}',we)
    for _ in range(120):
        if any(c.poll() is not None for c in children):raise RuntimeError('Cluster startup process exited')
        try:
            status=cluster()
            if status.get('aliveworkers')==2:break
        except OSError:pass
        time.sleep(1)
    else:raise RuntimeError('Cluster startup timeout')
    (out/'cluster-start.json').write_text(json.dumps(status,indent=2))
    (out/'diskstats-start.txt').write_text(pathlib.Path('/proc/diskstats').read_text())
    driver=launch(['numactl','--physcpubind=0-7','--membind=0',str(spark/'bin/spark-submit'),'--properties-file',str(conf_path),str(E/'scripts/benchmark_scale.py'),a.suite,str(out)],'driver',env)
    print('START',out.name,'driver',driver.pid,flush=True)
    with (out/'resources.jsonl').open('w') as monitor:
        while driver.poll() is None:
            m=meminfo(); sample={'elapsed':time.time()-start,'memory':m, 'vmstat':{line.split()[0]:int(line.split()[1]) for line in pathlib.Path('/proc/vmstat').read_text().splitlines() if line.startswith(('pswpin ', 'pswpout '))}}
            sample['numa_memory'] = {}
            for node in (0,1):
                entries = {}
                for line in pathlib.Path(f'/sys/devices/system/node/node{node}/meminfo').read_text().splitlines():
                    fields=line.split();entries[fields[2].rstrip(':')]=int(fields[3])*1024
                sample['numa_memory'][node]=entries
            sample['executors'] = []
            preflight=out/'executor-native-preflight.json'
            if preflight.exists():
                for info in json.loads(preflight.read_text()):
                    try:
                        fields=dict(x.split(':',1) for x in pathlib.Path(f"/proc/{info['pid']}/status").read_text().splitlines() if ':' in x)
                        sample['executors'].append({'pid':info['pid'],'numa_policy':info['numa_policy'],'rss':int(fields.get('VmRSS','0').split()[0])*1024,'swap':int(fields.get('VmSwap','0').split()[0])*1024})
                    except FileNotFoundError:pass
            sample['executor_swap_bytes']=sum(x['swap'] for x in sample['executors'])
            if sample['executor_swap_bytes'] > 0:
                (out/'memory-pressure-zoneinfo.txt').write_text(pathlib.Path('/proc/zoneinfo').read_text())
                (out/'memory-pressure-psi.txt').write_text(pathlib.Path('/proc/pressure/memory').read_text())
            monitor.write(json.dumps(sample)+'\n');monitor.flush()
            if m['MemAvailable']<12*1024**3 or sample['executor_swap_bytes']>1*1024**3:
                raise RuntimeError('Stopping benchmark because of host memory/swap pressure')
            if time.time()-start>7200:raise RuntimeError('Benchmark exceeded two hours')
            time.sleep(2)
    if driver.returncode:raise RuntimeError('Driver failed with exit '+str(driver.returncode))
    summary=json.loads((out/'summary.json').read_text())
    assert summary['status']=='success' and summary['spark_version']=='5.0.0-SNAPSHOT',summary
    assert summary['query_count']==(len(a.queries.split(',')) if a.queries else (24 if a.suite=='tpch' else 103)),summary
    oldref=OLD/'results'/f'full-pass2-{a.suite}-snappy'
    assert json.loads((oldref/'schemas.json').read_text())==json.loads((out/'schemas.json').read_text()),'Input table schemas changed'
    # The comparator requires matching query keys; create a filtered reference for smoke runs.
    compare_ref=oldref
    if a.queries:
        compare_ref=out/'reference';compare_ref.mkdir()
        qs=json.loads((oldref/'queries.json').read_text());names=set(a.queries.split(','))
        (compare_ref/'queries.json').write_text(json.dumps([q for q in qs if q['query'] in names]))
        for name in names:
            for suffix in ['-rows.json','-schema.json']:
                (compare_ref/(name+suffix)).symlink_to(oldref/(name+suffix))
    with (out/'correctness.json').open('w') as f:
        subprocess.run([str(R/'.deps/spark4-env/bin/python'),str(OLD/'scripts/compare-results.py'),str(compare_ref),str(out)],stdout=f,check=True)
    assert json.loads((out/'correctness.json').read_text())['equal']
    # Conservative timing-quality check: include startup/warmup; do not silently
    # publish a swapped run as a clean comparison. Correctness is separate.
    samples=[json.loads(line) for line in (out/'resources.jsonl').read_text().splitlines()]
    swap_activity={k:samples[-1]['vmstat'][k]-samples[0]['vmstat'][k] for k in ['pswpin','pswpout']}
    swap_io_bytes=sum(swap_activity.values())*os.sysconf('SC_PAGE_SIZE')
    timing_clean=swap_io_bytes <= 16*1024**2
    metadata['swap_activity_bytes']=swap_io_bytes
    metadata['maximum_swap_activity_bytes_for_timing']=16*1024**2
    metadata['swap_activity_pages']=swap_activity
    metadata['timing_quality_scope']='Host swap counters over whole application, including warmup; conservative, not per-task attribution.'
    if not timing_clean:
        metadata['performance_eligible']=False
        metadata['timing_invalid_reason']='Host swap I/O exceeded 16 MiB; keep correctness results, exclude performance comparison.'
    (out/'launch.json').write_text(json.dumps(metadata,indent=2))
    summary['whole_run_wall_seconds']=time.time()-start
    (out/'summary.json').write_text(json.dumps(summary,indent=2))
    print('DONE',out.name,json.dumps(summary),flush=True)
except BaseException as exc:
    metadata.update(performance_eligible=False, timing_invalid_reason=repr(exc))
    (out/'launch.json').write_text(json.dumps(metadata,indent=2))
    (out/'runner-failure.json').write_text(json.dumps({'error':repr(exc),'elapsed':time.time()-start},indent=2))
    raise
finally:
    (out/'diskstats-end.txt').write_text(pathlib.Path('/proc/diskstats').read_text())
    if os.environ.get('PGO_GCDA_WAIT'):
        # instrumented executors dump .gcda at exit; do not kill them mid-write
        t0=time.time()
        while time.time()-t0<1800:
            if not any(b'CoarseGrainedExecutorBackend' in (q/'cmdline').read_bytes() for q in pathlib.Path('/proc').iterdir() if q.name.isdigit() and (q/'cmdline').exists()):break
            time.sleep(2)
        (out/'gcda-wait.json').write_text(json.dumps({'waited':time.time()-t0}))
    for c in reversed(children):
        if c.poll() is None:os.killpg(c.pid,signal.SIGTERM)
    for c in reversed(children):
        try:c.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(c.pid,signal.SIGKILL);c.wait()
    for f in logs:f.close()
    # Delete only this run's allocated shuffle directories, after its processes stop.
    for dirs in shuffle.values():
        for d in dirs:
            path=pathlib.Path(d)
            assert path.parent.name==out.name and path.parent.parent.name=='ferd-vera-rtm-20261107'
            shutil.rmtree(path)
