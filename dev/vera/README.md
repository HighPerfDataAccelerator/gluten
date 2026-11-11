# Vera (NVIDIA Olympus, aarch64, 352 logical CPUs) SF1K tooling

Everything needed to reproduce the Vera NDS / NDS-H SF1K numbers and the RTM experiments, besides the
engine source in this branch and in winningsix/spark `rtm-vera-fixes`.

## Best measured config (Spark 4.0.1 + Gluten/Velox, 42 executors x 8 cores)

| Suite | 3 apps (s) | mean | vs Turin 128c (193 / 122.7 s) |
|---|---|---|---|
| NDS SF1K | 128.47 / 128.58 / 130.26 | 129.10 | 1.495x (best run 1.502x) |
| NDS-H SF1K | 75.49 / 76.60 / 75.63 | 75.90 | 1.616x |

Whole-machine CPU busy 41.7% (NDS) / 55.8% (NDS-H). AB experiments 571928729654621071 (NDS) and
807064273964984679 (NDS-H).

Ingredients:
- `vera_best_conf.json`: Spark/Gluten conf (stateless validation, minPartitionSize 64KB, scan coalescing
  384m/8m, scheduler keys, `spark.sql.shuffle.partitions=200`).
- Stateless plan validation: JVM side `org.apache.gluten.experimental.StatelessPlanValidation` +
  `VeloxValidatorApi`, native side `Java_org_apache_gluten_experimental_StatelessPlanValidation_nativeValidate`
  in `cpp/velox/jni/VeloxJniWrapper.cc` (both in this branch). The original stand-alone patch against
  `arm-sve-optimizations-20261024` is kept in `patches/stateless-validation.patch`.
- libvelox: Velox `be88416a3` (HighPerfDataAccelerator/velox `arm-sve-optimizations-20261024`) +
  `patches/velox-join-build-window128.patch`, built with gcc 15.2 PGO (`pgo/`; training = 32 NDS + 10 NDS-H
  queries, so in-sample).

## Layout
- `patches/`: Velox join-build window patch; stateless validation patch (and the patched JNI file it came from).
- `pgo/`: CMake cache and helper scripts used for the gcc `-fprofile-generate` / `-fprofile-use` libvelox build
  and for repacking the gluten jar with the PGO library.
- `harness/`: strict one-app-at-a-time runner (`run-monitored-strict.py`, `run-suite-strict.py`), the partition
  sweeps that produced the p200 result (`run-sweep5/6/7.py`), and `summarize.py`.
- `ab/prepare-results.py`: converts harness results to the AB import JSON.
- `analysis/`: slot-time scheduling breakdown (`sched_breakdown.py`, `short_split.py`), driver no-job gap split
  (`driver_gaps.py`), perfect-packing bound for pipelined shuffle (`barrier_bound.py`), driver JFR tools
  (`jfr-attach.sh`, `jfr_driver.py`, `jfr_thread.py`, `jfr_callers*.py`).
- `rtm/`: RTM pipelined-shuffle experiment confs and runners (Spark 5.0.0-SNAPSHOT from winningsix/spark
  `rtm-vera-fixes` + this branch).

## RTM notes
Works end to end only with: prepared receive service + `spark.shuffle.streaming.groupAdmission.enabled`,
writer/reader `waitForTerminationAcks=false`, `networkBufferSize` <= 256KB (1MB exhausts the 4GB raw buffer
pool and deadlocks), and the Spark-side rule that keeps range-sort exchanges and their child frontier regular.
Result on Vera: Q3 13.0 s / Q10 10.0 s vs BSP 5.2 / 4.0 s, because ~250 resident readers leave producers 86 of
336 slots (`conf-on3.json`; oversubscribed `conf-on-os*.json` is slower still).

Scripts contain absolute paths of the Vera host (`/home/nvidia/ferd`, `/data/nvme1`); adjust before reuse.
