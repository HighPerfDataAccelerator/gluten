import os
import hashlib
import json
import pathlib
import sys
import time
import traceback
from pyspark.sql import SparkSession
from perf_capture import PerfCapture

suite, output_arg = sys.argv[1:3]
suite = {'tpcds': 'nds', 'tpch': 'nds-h'}.get(suite, suite)
if suite not in ('nds', 'nds-h'):
    raise ValueError('Expected tpcds/tpch or nds/nds-h')
out = pathlib.Path(output_arg)
out.mkdir(parents=True, exist_ok=True)
root = pathlib.Path(__file__).resolve().parent
queries = json.loads((root / (suite+'-queries.json')).read_text())
selected = os.environ.get('TPC_QUERIES', '')
if selected:
    names = selected.split(',')
    unknown = set(names) - queries.keys()
    if unknown:
        raise ValueError(f'Unknown query names: {sorted(unknown)}')
    queries = {name: queries[name] for name in names}
(out / 'selected-queries.json').write_text(json.dumps(queries, indent=2))
repetitions = int(os.environ.get('TPC_QUERY_REPETITIONS', '1'))
if repetitions < 1:
    raise ValueError('TPC_QUERY_REPETITIONS must be positive')
if repetitions > 1:
    queries = {f'{name}__repeat{rep}': sql
               for rep in range(1, repetitions + 1)
               for name, sql in queries.items()}
(out / 'query-repetitions.json').write_text(json.dumps({
    'repetitions': repetitions, 'order': 'complete selected query list per repetition',
    'warmup': 'No automatic exclusion; report first and subsequent repetitions separately.'}, indent=2))

dataset = 'tpcds' if suite == 'nds' else 'tpch'
data_roots = [pathlib.Path(x) for x in os.environ.get('TPC_DATA_ROOTS', ','.join(f'/data/nvme{n}/{dataset}/sf1000' for n in [1,2,3,5,6,7])).split(',')]
for path in data_roots:
    if not path.is_dir():
        raise FileNotFoundError(path)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version == '5.0.0-SNAPSHOT', spark.version
expected_executors=int(os.environ['TPC_EXECUTOR_COUNT']);assert expected_executors in (31,42)
# Do not let startup/ramp-up run the first query with a partial executor pool.
for _ in range(120):
    registered = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    if registered == expected_executors:
        break
    time.sleep(1)
else:
    raise RuntimeError(f'Expected {expected_executors} executors; registered {registered}')
(out / 'executor-preflight.json').write_text(json.dumps({'registered': registered, 'expected': expected_executors, 'cores_each': int(os.environ.get('TPC_EXECUTOR_CORES','8')), 'spark_version':spark.version},indent=2))

expected_package = os.environ.get('TPC_NATIVE_PACKAGE')
if expected_package:
    expected = json.loads(pathlib.Path(expected_package).read_text())['native_libraries']
    expected = {pathlib.Path(path).name: digest for path, digest in expected.items()}
    jvm_pid = spark._jvm.java.lang.ProcessHandle.current().pid()
    mapped_text = pathlib.Path(f'/proc/{jvm_pid}/maps').read_text()
    (out / 'jvm-native-maps.txt').write_text(mapped_text)
    paths = {line.split()[-1] for line in mapped_text.splitlines() if '/' in line}
    libraries = []
    for path in sorted(paths):
        if pathlib.Path(path).name in expected:
            with open(path, 'rb') as library:
                digest = hashlib.file_digest(library, 'sha256').hexdigest()
            libraries.append({'path': path, 'sha256': digest})
    assert len(libraries) == len(expected), (libraries, expected)
    assert all(expected[pathlib.Path(item['path']).name] == item['sha256'] for item in libraries), (libraries, expected)
    (out / 'loaded-native-libraries.json').write_text(json.dumps(libraries, indent=2))
    assert any('libjemalloc.so.2' in path for path in paths), 'jemalloc not loaded in driver'

    # Sample one executor per NUMA worker before timing; retain all executor affinities.
    executor_records = []
    sampled_nodes = set()
    own_tmp = str(out / 'runtime' / 'tmp').encode()
    for process in pathlib.Path('/proc').iterdir():
        if not process.name.isdigit():
            continue
        try:
            command = (process / 'cmdline').read_bytes()
            if b'CoarseGrainedExecutorBackend' not in command or own_tmp not in command:
                continue
            status = dict(line.split(':',1) for line in (process/'status').read_text().splitlines() if ':' in line)
            cpus = status['Cpus_allowed_list'].strip()
            assert cpus in ('8-87,184-263','88-175,264-351'), cpus
            item = {'pid':int(process.name),'cpus_allowed':cpus,'mems_allowed':status['Mems_allowed_list'].strip()}
            numa_text = (process/'numa_maps').read_text()
            expected_policy = 'bind:0' if cpus == '8-87,184-263' else 'bind:1'
            item['numa_policy'] = expected_policy
            assert any(line.split()[1] == expected_policy for line in numa_text.splitlines()), (item, numa_text[:500])

            if cpus not in sampled_nodes:
                # Executor registration can precede completion of native loading.
                # Wait only before timing; keep the exact native hash checks below.
                readiness_start = time.monotonic()
                while True:
                    paths = {line.split()[-1] for line in (process/'maps').read_text().splitlines() if '/' in line}
                    names = {pathlib.Path(path).name for path in paths}
                    if set(expected).issubset(names):
                        break
                    if time.monotonic() - readiness_start >= 60:
                        raise RuntimeError(f'Native load readiness timeout for executor {process.name}: {set(expected) - names}')
                    time.sleep(0.1)
                item['native_readiness_wait_seconds'] = time.monotonic() - readiness_start
                assert any('libjemalloc.so.2' in path for path in paths), 'jemalloc not loaded in executor'
                verified = {}
                for path in paths:
                    name = pathlib.Path(path).name
                    if name in expected:
                        with open(path,'rb') as f:
                            verified[name] = hashlib.file_digest(f,'sha256').hexdigest()
                assert verified == expected, (item, verified, expected)
                item['native_sha256'] = verified
                sampled_nodes.add(cpus)
            executor_records.append(item)
        except (FileNotFoundError,ProcessLookupError,PermissionError):
            continue
    assert len(executor_records) == expected_executors and len(sampled_nodes) == 2, executor_records
    (out/'executor-native-preflight.json').write_text(json.dumps(executor_records,indent=2))
    # Read only the experiment variable; never persist unrelated process env.
    key = b'VELOX_EXPERIMENTAL_PACKED_GROUP_KEYS='
    setting = spark.conf.get('spark.executorEnv.VELOX_EXPERIMENTAL_PACKED_GROUP_KEYS')
    assert setting in ('0', '3')
    def experiment_environment(pid):
        entries = pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')
        values = [entry[len(key):].decode() for entry in entries if entry.startswith(key)]
        assert values == [setting], (pid, values, setting)
        return values[0]
    activation = dict(expected=setting, driver=experiment_environment(jvm_pid),
        executors=[dict(pid=x['pid'], value=experiment_environment(x['pid'])) for x in executor_records],
        limitation='Configuration propagation and loaded artifact, not a dynamic SVE instruction count.')
    (out/'packed-key-environment.json').write_text(json.dumps(activation,indent=2))




schemas = {}
table_names = sorted({p.name for base in data_roots for p in base.iterdir() if p.is_dir() and not p.name.startswith(('_', '.'))})
for table in table_names:
    paths = [str(base / table) for base in data_roots if (base / table).is_dir() and any((base / table).rglob('*.parquet'))]
    if not paths:
        raise RuntimeError(f'No Parquet files for {table}')
    # Read each table root separately so Spark retains Hive partition columns.
    df = spark.read.parquet(paths[0])
    for path in paths[1:]:
        df = df.unionByName(spark.read.parquet(path))
    schemas[table] = df.schema.jsonValue()
    df.createOrReplaceTempView(table)
(out / 'data-roots.json').write_text(json.dumps(list(map(str, data_roots)), indent=2))
(out / 'schemas.json').write_text(json.dumps(schemas, indent=2))
(out / 'effective-conf.json').write_text(json.dumps(dict(spark.sparkContext.getConf().getAll()), indent=2))
cache_policy = os.environ.get('TPC_CACHE_POLICY', 'retain')
if cache_policy not in ('retain', 'advisory-evict-per-query'):
    raise ValueError(f'Unsupported cache policy: {cache_policy}')
parquet_files = [p for base in data_roots for p in base.rglob('*.parquet')] if cache_policy == 'advisory-evict-per-query' else []
(out / 'cache-policy.json').write_text(json.dumps({'policy': cache_policy, 'file_count': len(parquet_files), 'note': 'POSIX_FADV_DONTNEED requests file page-cache eviction; device caches are not controlled.'}, indent=2))
def plan_metrics(plan):
    seen = set()
    def walk(node):
        identity = spark._jvm.java.lang.System.identityHashCode(node)
        if identity in seen:
            return {'ref': identity}
        seen.add(identity)
        result = {'id': identity, 'node': node.nodeName(), 'class': node.getClass().getSimpleName(), 'metrics': {}}
        metrics = node.metrics().iterator()
        while metrics.hasNext():
            pair = metrics.next()
            metric = pair._2()
            result['metrics'][pair._1()] = {'name': str(metric.name()), 'value': metric.value(), 'type': metric.metricType()}
        children = node.children().iterator()
        result['children'] = []
        while children.hasNext():
            result['children'].append(walk(children.next()))
        if result['class'] == 'AdaptiveSparkPlanExec':
            result['children'].append(walk(node.executedPlan()))
        elif result['class'] in ('ShuffleQueryStageExec', 'BroadcastQueryStageExec', 'ResultQueryStageExec'):
            result['children'].append(walk(node.plan()))
        return result
    return walk(plan)

results = []
try:
    # Warm the selected workloads before sampling to reduce JIT/startup noise.
    if os.environ.get('TPC_PERF_PROFILE') == '1' or os.environ.get('TPC_WARMUP') == '1':
        warmups = []
        for name, sql in queries.items():
            spark.sparkContext.setJobGroup('profile-warmup-'+name, name)
            started = time.perf_counter()
            count = len(spark.sql(sql).collect())
            warmups.append({'query': name, 'seconds': time.perf_counter()-started,
                            'row_count': count})
            (out/'profile-warmups.json').write_text(json.dumps(warmups, indent=2))
    for name, sql in queries.items():
        spark.sparkContext.setJobGroup(name, name)
        for parquet_file in parquet_files:
            fd = os.open(parquet_file, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
        capture = PerfCapture(out, name, executor_records) if os.environ.get("TPC_PERF_PROFILE") == "1" else None
        if capture:
            capture.start()
        start = time.time()
        start_monotonic = time.perf_counter()
        try:
            frame = spark.sql(sql)
            rows = frame.collect()
            elapsed = time.perf_counter() - start_monotonic
            end = time.time()
            if capture:
                capture.stop()
            (out / (name+'-schema.json')).write_text(frame.schema.json())
            try:
                metrics = plan_metrics(frame._jdf.queryExecution().executedPlan())
                (out / (name+'-metrics.json')).write_text(json.dumps(metrics, indent=2))
            except Exception:
                (out / (name+'-metrics-error.txt')).write_text(traceback.format_exc())
            (out / (name+'-plan.txt')).write_text(frame._jdf.queryExecution().toString())
            (out / (name+'-executed-plan.txt')).write_text(frame._jdf.queryExecution().executedPlan().toString())
            jvm_pid = spark._jvm.java.lang.ProcessHandle.current().pid()
            maps = pathlib.Path(f'/proc/{jvm_pid}/maps')
            if maps.exists():
                mapped_text = maps.read_text()
                (out / 'jvm-native-maps.txt').write_text(mapped_text)
                native_metadata = out / 'loaded-native-libraries.json'
                if not native_metadata.exists():
                    libraries = []
                    paths = {line.split()[-1] for line in mapped_text.splitlines() if '/' in line}
                    for path in sorted(paths):
                        if pathlib.Path(path).name in ('libgluten.so', 'libvelox.so', 'libsnappy.so'):
                            with open(path, 'rb') as library:
                                digest = hashlib.file_digest(library, 'sha256').hexdigest()
                            libraries.append({'path': path, 'sha256': digest})
                    native_metadata.write_text(json.dumps(libraries, indent=2))
            result = {'status': 'success', 'query': name, 'seconds': elapsed, 'start': start, 'end': end, 'row_count': len(rows), 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest()}
            # Serialize after the timer; retain results for independent correctness checks.
            (out / (name+'-rows.json')).write_text(json.dumps([r.asDict(recursive=True) for r in rows], default=str, sort_keys=True))
        except Exception:
            if capture:
                capture.stop()
            failure = traceback.format_exc()
            (out / (name+'-failure.txt')).write_text(failure)
            result = {'status': 'failed', 'query': name, 'seconds': time.perf_counter()-start_monotonic, 'start': start, 'end': time.time(), 'row_count': None, 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest()}
            spark.sparkContext.cancelJobGroup(name)
            if spark.sparkContext._jsc.sc().isStopped():
                raise
        results.append(result)
        (out / 'queries.json').write_text(json.dumps(results, indent=2))
        print('QUERY_RESULT '+json.dumps(result), flush=True)
        if result['status'] != 'success':
            raise RuntimeError('First SQL failure; stop without replacement timing: '+name)
    # Audit activation once after all SQL timers; no diagnostic helper in scored applications.
    enabled = spark.conf.get('spark.gluten.experimental.statelessValidation.enabled', 'false') == 'true'
    activation = dict(enabled=enabled,verification_enabled=False)
    if enabled:
        activation['native_validation_calls'] = spark._jvm.org.apache.gluten.experimental.StatelessPlanValidation.validationCount()
        activation['equivalence_checks'] = spark._jvm.org.apache.gluten.experimental.StatelessPlanValidation.equivalenceCount()
        assert activation['native_validation_calls'] > 0 and activation['equivalence_checks'] == 0
    (out/'stateless-activation.json').write_text(json.dumps(activation,indent=2))
    (out / 'summary.json').write_text(json.dumps({'status': 'success' if all(q['status']=='success' for q in results) else 'query_failures','suite':suite,'query_count':len(results),'query_seconds':sum(q['seconds'] for q in results if q['status']=='success'),'spark_version':spark.version,'application_id':spark.sparkContext.applicationId}, indent=2))
    if any(q['status']=='failed' for q in results):
        raise RuntimeError('One or more queries failed; see per-query failure files')
except Exception:
    (out / 'failure.txt').write_text(traceback.format_exc())
    raise
finally:
    spark.stop()
