#!/usr/bin/env python3
"""Aggregate driver JFR execution samples: by thread, by top Spark/Gluten frame, by phase bucket.

usage: jfr_driver.py <jfr binary> <recording.jfr>
"""
import json
import subprocess
import sys
from collections import Counter

jfr_bin, rec = sys.argv[1], sys.argv[2]
out = subprocess.run([jfr_bin, 'print', '--json', '--stack-depth', '96', '--events', 'jdk.ExecutionSample', rec],
                     capture_output=True, text=True, check=True).stdout
events = json.loads(out)['recording']['events']

BUCKETS = [  # first match wins, searched over the whole stack (leaf -> root)
    ('gluten-validate', ('Validator', 'validateNative', 'StatelessPlanValidation', 'doValidate')),
    ('gluten-transform', ('org.apache.gluten.extension', 'org.apache.gluten.planner', 'ColumnarRuleApplier',
                          'org.apache.gluten.execution', 'TransformHint', 'HeuristicTransform', 'RasOffload')),
    ('aqe-reoptimize', ('reOptimize', 'AdaptiveSparkPlanExec.newQueryStage', 'createQueryStages',
                        'applyPhysicalRules', 'AQEOptimizer')),
    ('catalyst-optimize', ('org.apache.spark.sql.catalyst.optimizer', 'RuleExecutor', 'Analyzer')),
    ('planning', ('QueryPlanner', 'SparkPlanner', 'EnsureRequirements')),
    ('broadcast', ('Broadcast', 'TorrentBroadcast')),
    ('collect/result', ('collectToPython', 'executeCollect', 'SerDeUtil', 'PythonRDD', 'decodeUnsafeRows',
                        'ColumnarToRow', 'serveToStream')),
    ('scheduler', ('org.apache.spark.scheduler',)),
    ('listener/eventlog', ('EventLoggingListener', 'AsyncEventQueue', 'JsonProtocol', 'LiveListenerBus',
                           'SQLAppStatusListener', 'AppStatusListener')),
    ('rpc/netty', ('io.netty', 'org.apache.spark.rpc', 'NettyRpc')),
    ('file-listing', ('InMemoryFileIndex', 'listLeafFiles', 'HadoopFSUtils', 'FileStatus', 'PartitioningAwareFileIndex')),
    ('gc/jit/other', ()),
]


def frames(e):
    return [f['method']['type']['name'] + '.' + f['method']['name'] for f in e['values']['stackTrace']['frames']]


by_thread, by_bucket, top_frame = Counter(), Counter(), Counter()
bucket_thread = Counter()
for e in events:
    t = (e['values'].get('sampledThread') or {}).get('javaName') or '?'
    tname = t.split('-')[0] if t.startswith(('Executor', 'dispatcher', 'shuffle', 'rpc', 'task-result', 'block-manager')) else t
    by_thread[tname] += 1
    fs = frames(e)
    stack = ' '.join(fs)
    b = next(name for name, keys in BUCKETS if not keys or any(k in stack for k in keys))
    by_bucket[b] += 1
    bucket_thread[(b, tname)] += 1
    first = next((f for f in fs if f.startswith(('org.apache.spark', 'org.apache.gluten'))), fs[0] if fs else '?')
    top_frame[first] += 1

n = len(events)
print(f'{n} execution samples (~20 ms each under profile settings, per running thread)')
print('\nby thread:')
for k, v in by_thread.most_common(12):
    print(f'  {v:6d} {100 * v / n:5.1f}%  {k}')
print('\nby bucket:')
for k, v in by_bucket.most_common():
    print(f'  {v:6d} {100 * v / n:5.1f}%  {k}')
print('\nbucket x thread (top 15):')
for (b, t), v in bucket_thread.most_common(15):
    print(f'  {v:6d}  {b:18s} {t}')
print('\ntop first Spark/Gluten frames:')
for k, v in top_frame.most_common(25):
    print(f'  {v:6d} {100 * v / n:5.1f}%  {k}')
