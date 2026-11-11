#!/usr/bin/env python3
"""Convert the Vera p200 best-config apps (vera-confsweep-20261106, shuffle.partitions=200 on the stacked
best config) to AB's JSON import schema.

All three non-instrumented p200 apps per suite are published as runs 1..3 of one experiment (no selection).
The JFR-attached app pg00dsp200 is excluded because a profiler was attached to the driver.
"""
import hashlib, json, pathlib, socket, sqlite3, sys

root = pathlib.Path('/home/nvidia/ferd')
camp = root / 'benchmarks/vera-confsweep-20261106'
stack = root / 'benchmarks/vera-stack-20261106'
out = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(root / 'code/ab/frontend'))
from database import BenchmarkDatabase

LABELS = {'tpcds': ['pe03dsp200', 'pe07dsp200', 'pf00dsp200'], 'tpch': ['pe03chp200', 'pe07chp200', 'pf00chp200']}
TURIN = {'tpcds': 193.0, 'tpch': 122.7}


def read(p):
    return json.loads(pathlib.Path(p).read_text())


def digest(p):
    return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()


def identifier(kind, value):
    text = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return int(hashlib.sha256(('mgx-vera-c2-100/p200/20261109/' + kind + '/' + text).encode()).hexdigest()[:15], 16)


package = read(stack / 'pgo/package.json')
PGO_JAR = package['jar_sha256']
assert PGO_JAR == '6ba16c52e852e59d5edecf5b010806128f892b65854c941dabc0dadb54607baa'
assert digest(camp / 'pgo/gluten.jar') == PGO_JAR
FLAGS = ('GCC 15.2 -O3 -DNDEBUG -mcpu=olympus -fno-stack-protector -fno-stack-clash-protection -U_FORTIFY_SOURCE '
         '-fzero-init-padding-bits=standard; libvelox PGO: -fprofile-use -fprofile-correction (training: 32 NDS + 10 NDS-H queries, in-sample)')

manifest = []
for suite, suite_name, n_queries in [('tpcds', 'nds', 103), ('tpch', 'nds-h', 24)]:
    runs_data = []
    for label in LABELS[suite]:
        d = camp / 'results' / f'{label}-{suite}-candidate'
        queries = read(d / 'queries.json')
        assert len(queries) == n_queries and all(q['status'] == 'success' for q in queries), label
        audit = read(camp / f'{label}-audit.json')['audit']
        assert audit['correct'] and audit['performance_eligible'] and not audit['profiled'], label
        assert audit['executors'] == 42 and audit['slots'] == 336, label
        assert audit['whole_app_host_swap_bytes'] <= 16 << 20, label
        corr = read(d / 'correctness.json')
        assert all(q['equal'] for q in corr['queries']) and len(corr['queries']) == n_queries, label
        sched = read(camp / f'sched-{label}-status.json')['runs']
        assert len(sched) == 1 and sched[0]['correct'], label
        sq = sched[0]['queries']
        busy = sum(q['seconds'] * q['host_busy_percent'] for q in sq) / sum(q['seconds'] for q in sq)
        runs_data.append((label, d, queries, busy))
    runs_data.sort(key=lambda x: min(q['start'] for q in x[2]))
    first_dir = runs_data[0][1]
    parameters = read(first_dir / 'effective-conf.json')
    assert parameters['spark.sql.shuffle.partitions'] == '200'

    def norm(conf, label):
        skip = ('spark.app.', 'spark.driver.port')
        return {k: str(v).replace(label, 'LABEL') for k, v in conf.items() if not k.startswith(skip)}
    base_norm = norm(parameters, runs_data[0][0])
    for lab, d, _q, _b in runs_data[1:]:
        other = norm(read(d / 'effective-conf.json'), lab)
        diff = {k for k in set(base_norm) | set(other) if base_norm.get(k) != other.get(k)}
        assert not diff, ('effective conf differs between runs beyond per-app identifiers', sorted(diff))
    for _, d, _q, _b in runs_data:
        got = {pathlib.Path(x['path']).name: x['sha256'] for x in read(d / 'loaded-native-libraries.json')}
        want = {pathlib.Path(k).name: v for k, v in package['native_libraries'].items()}
        assert got == want, (d, got, want)
    totals = {x[0]: sum(q['seconds'] for q in x[2]) for x in runs_data}
    mean = sum(totals.values()) / len(totals)
    best = min(totals.values())
    busy = {x[0]: round(x[3], 1) for x in runs_data}
    target = TURIN[suite] / 1.5
    roots = read(first_dir / 'data-roots.json')
    notes = [
        'Imported after execution from original per-query records of the Vera partition sweep (vera-confsweep-20261106, sweep5 ABBA apps pe03/pe07 + sweep6 confirmation app pf00); not run by the AB execution harness.',
        'All three non-instrumented apps of this config are published as runs 1..3, no best-of, no dropped samples. One suite pass per app, zero warmup. '
        'Excluded: pg00dsp200 (same config, JFR profiler attached to the driver, 129.62 s).',
        'Config = Vera stacked best (statelessValidation=true, coalescePartitions.minPartitionSize=64KB, 6 scheduler/UI keys, '
        'maxPartitionBytes=384m, openCostInBytes=8m, gcc PGO libvelox.so, jar sha256 ' + PGO_JAR + ') + spark.sql.shuffle.partitions=200 '
        '(the only effective-conf difference vs the stacked best published as experiments 490659182614178239 / 798960913773360542).',
        f'Totals (s): {json.dumps({k: round(v, 2) for k, v in totals.items()})}; mean {mean:.2f}, best {best:.2f}.',
        f'vs Turin 128c reference {TURIN[suite]:.1f} s: mean {TURIN[suite] / mean:.3f}x, best run {TURIN[suite] / best:.3f}x (1.5x target <= {target:.1f} s).',
        f'Whole-machine CPU busy (query-time weighted, 352 logical CPUs) per app: {json.dumps(busy)} -- the >=80% CPU goal is NOT met.',
        'PGO training queries are a subset of the benchmark queries, so the PGO part of the gain is in-sample.',
        'Gates passed for all apps: MemAvailable>=12 GiB, executor VmSwap<=1 GiB, host swap IO<=16 MiB per app, OOM 0, one app at a time, rows equal to reference (row multiset).',
    ]
    metadata = {
        'source_campaign': str(camp), 'run_labels': [x[0] for x in runs_data],
        'source_queries_sha256': {x[0]: digest(x[1] / 'queries.json') for x in runs_data},
        'source_spark_application_ids': {x[0]: read(x[1] / 'summary.json')['application_id'] for x in runs_data},
        'jar_sha256': PGO_JAR, 'control_jar_sha256': package['control_jar_sha256'],
        'native_sha256': package['native_libraries'],
        'velox_source': 'velox-before-decoder be88416a3 + join-build-window128.patch (as the control libvelox), rebuilt with gcc PGO',
        'executor_topology': '42 executors x 8 cores = 336 task slots, NUMA bound, driver CPUs 0-7',
        'partition_floor': 248, 'shuffle_partitions': 200, 'input_roots': roots,
        'turin_reference_seconds': TURIN[suite], 'cpu_busy_percent': busy, 'notes': notes,
    }
    sut = {'s_name': 'Spark SQL / Gluten Velox'}
    hw = {'h_hostname': socket.gethostname(), 'h_cpu_arch': 'aarch64', 'h_cpu_model_name': 'NVIDIA Olympus',
          'h_cpu_physical_cores': 176, 'h_cpu_logical_cores': 352}
    build = {'b_version': '1.8.0-SNAPSHOT',
             'b_revision': 'velox be88416a3 + join-build-window128.patch, gcc PGO libvelox; artifact-sha256:' + PGO_JAR,
             'b_compiler_flags': FLAGS}
    data = {'d_storage_device_kind': 'disk', 'd_format': 'parquet', 'd_location': ','.join(roots),
            'd_char_type': 'string', 'd_decimal_type': 'decimal'}
    config = {'p_template_path': str(camp / 'conf-p200.json'),
              'p_config_json': json.dumps({'spark_configs': parameters, 'environment_variables': {}, 'benchmark_metadata': metadata}, sort_keys=True)}
    for kind, value, prefix in [('sut', sut, 's'), ('hardware', hw, 'h'), ('build', build, 'b'), ('data', data, 'd'), ('config', config, 'p')]:
        value[prefix + '_id'] = identifier(kind, value)
    sut['s_id'] = 899362420033334724  # existing server-side row for 'Spark SQL / Gluten Velox' (UNIQUE s_name)
    eid = identifier('experiment', [suite, PGO_JAR, [digest(x[1] / 'queries.json') for x in runs_data]])
    exp = {'e_id': eid, 'e_sut_info_id': sut['s_id'], 'e_parameters_id': config['p_id'], 'e_hw_info_id': hw['h_id'],
           'e_build_info_id': build['b_id'], 'e_data_info_id': data['d_id'],
           'e_name': f'Vera SF1K {suite_name.upper()} Gluten/Velox - 42x8, best config: stacked levers + shuffle.partitions=200',
           'e_suite': suite_name, 'e_platform': 'local', 'e_accelerator': 'gluten',
           'e_driver_instance_type': 'NVIDIA Olympus', 'e_instance_type': 'NVIDIA Olympus', 'e_instance_count': 1,
           'e_scale_factor': 1000.0, 'e_query_source': 'sql', 'e_benchmark_type': 'test', 'e_username': 'ferdinandx',
           'e_version': 'Spark 4.0.1 / Gluten 1.8.0-SNAPSHOT / aarch64',
           'e_tags': 'vera,sf1k,smt-on,42x8,336-slots,velox-be88416a3,gcc-pgo,scan-coalesce,stateless-validation,shuffle-partitions-200,2026-11-09,three-apps-all-published',
           'e_timestamp': int(min(q['start'] for q in runs_data[0][2]))}
    runs, rows = [], []
    for n, (label, d, queries, b) in enumerate(runs_data, 1):
        rid = identifier('run', [eid, label])
        total = sum(q['seconds'] for q in queries)
        runs.append({'r_id': rid, 'r_experiment_id': eid, 'r_number': n, 'r_total_duration_s': total, 'r_query_count': len(queries),
                     'r_description': f'{label}: shuffle.partitions=200 best config, 1 full suite, 0 warmups; SQL planning + collect; '
                                      f'correctness passed; all gates passed; CPU busy {b:.1f}%.'})
        rows += [{'q_id': identifier('query', [rid, i, q['query']]), 'q_run_id': rid, 'q_name': q['query'],
                  'q_nvtx_marker': q['query'], 'q_duration_s': q['seconds'], 'q_sequence': i} for i, q in enumerate(queries, 1)]
    payload = {'sut_info': [sut], 'hw_info': [hw], 'build_info': [build], 'data_info': [data], 'parameters_info': [config],
               'experiment': [exp], 'run': runs, 'query': rows}
    path = out / f'{suite_name}-p200-published-results.json'
    path.write_text(json.dumps(payload, indent=2))
    manifest.append({'suite': suite_name, 'experiment_id': str(eid), 'runs': {r['r_number']: r['r_total_duration_s'] for r in runs},
                     'mean': mean, 'best': best, 'cpu_busy': busy,
                     'query_count': n_queries, 'payload': str(path), 'payload_sha256': digest(path),
                     'url': f'https://ab.nvidia.com/?experiment_id={eid}'})

db_path = out / 'validation.sqlite'
if db_path.exists():
    db_path.unlink()
db = BenchmarkDatabase(str(db_path))
for entry in manifest:
    st = db.insert_json_data(read(entry['payload']))
    assert not any(st['conflicts'].values()), st
with sqlite3.connect(db_path) as c:
    assert c.execute('PRAGMA foreign_key_check').fetchall() == []
    for entry in manifest:
        for rid, total in c.execute('SELECT r_number, r_total_duration_s FROM run WHERE r_experiment_id=?', (int(entry['experiment_id']),)).fetchall():
            q = c.execute('SELECT count(*),sum(q_duration_s) FROM query JOIN run ON q_run_id=r_id WHERE r_experiment_id=? AND r_number=?', (int(entry['experiment_id']), rid)).fetchone()
            assert q[0] == entry['query_count'] and abs(q[1] - total) < 1e-8
(out / 'manifest.json').write_text(json.dumps({'status': 'prepared_and_locally_validated', 'experiments': manifest}, indent=2))
print(json.dumps(manifest, indent=2))
