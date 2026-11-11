# Reproducing the Vera SF1K results

There are two different setups. They need **different Spark builds and different Gluten jars**, and the jars
are not interchangeable. Mixing them is the most likely cause of
`ClassCastException: ColumnarBatch cannot be cast to InternalRow` (see [Troubleshooting](#troubleshooting)).

| | A. Best config (the AB numbers) | B. RTM pipelined shuffle (experimental) |
|---|---|---|
| Result | NDS 129.10 s, NDS-H 75.90 s (mean of 3) | 2.5x **slower** than BSP on Q3/Q10 |
| Spark | Apache Spark **4.0.1** (pip `pyspark==4.0.1` used as `SPARK_HOME`) | **5.0.0-SNAPSHOT** built from winningsix/spark `rtm-vera-fixes` |
| Gluten source | HighPerfDataAccelerator/gluten `arm-sve-optimizations-20261024` (`3673833c2`) or `93c4221d3`, + `patches/gluten-stateless-validation.patch` | this branch, `rtm-vera` (JVM only) |
| Gluten maven profile | `-Pspark-4.0 -Pscala-2.13 -Pjava-17` | `-Pspark-4.2 -Pscala-2.13 -Pjava-17 -Dspark.version=5.0.0-SNAPSHOT` |
| Velox | HighPerfDataAccelerator/velox `arm-sve-optimizations-20261024` (`be88416a3`) + `patches/velox-join-build-window128.patch` | native libs copied from the A jar |
| Spark conf | `conf/vera-p200.conf` | `conf/vera-rtm-on.conf` |
| Exact binary | `gluten.jar` sha256 `6ba16c52e852e59d5edecf5b010806128f892b65854c941dabc0dadb54607baa` | built per B below |

Do **not** run the `rtm-vera` jar on Spark 4.0.x, and do not run a `-Pspark-4.0` jar on the RTM Spark 5.0 build.

## Common environment (both setups)

- Host: NVIDIA Vera (Olympus), aarch64, 2 NUMA nodes, 176 cores / 352 logical CPUs, 504 GiB RAM.
- JDK 17, Python 3.11, Scala 2.13, `numactl`, jemalloc (`/usr/lib/aarch64-linux-gnu/libjemalloc.so.2`).
- Spark standalone, one host, `SPARK_LOCAL_IP=127.0.0.1`:
  - master on CPUs 176-183 (`numactl --physcpubind=176-183 --membind=0`)
  - worker 0: CPUs `8-87,184-263`, `--membind=0`, `--cores 160 --memory 680G` (20 executors)
  - worker 1: CPUs `88-175,264-351`, `--membind=1`, `--cores 176 --memory 748G` (22 executors)
  - driver: `numactl --physcpubind=0-7 --membind=0 spark-submit --properties-file <conf> ...`
  - 42 executors x 8 cores = 336 slots; executor 6g heap + 22g off-heap + 6g overhead; driver 16g.
  - `SPARK_LOCAL_DIRS` / `spark.local.dir`: NUMA-local NVMe dirs per worker (Vera: node0 `/data/nvme1`,`/data/nvme6`; node1 `/data/nvme2`,`/data/nvme3`).
- Executor/driver env: `LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2`,
  `MALLOC_CONF=background_thread:true,narenas:8,dirty_decay_ms:1000,muzzy_decay_ms:1000`,
  `VELOX_EXPERIMENTAL_PACKED_GROUP_KEYS=0`.
- Data: Parquet SF1000, one directory per table (Hive-style partitions inside), e.g.
  `<root>/tpcds/sf1000/store_sales/ss_sold_date_sk=.../*.parquet`. On Vera:
  `/home/nvidia/ferd/benchmarks/baseline-alignment-20261029/views/{tpcds,tpch}/sf1000`.
- Queries: `rtm/scripts/nds-queries.json` (103 NDS queries incl. parts) and `nds-h-queries.json` (24).

The conf files in `conf/` are the full effective confs of passing runs, with per-run values removed. Add:

```
spark.master                     spark://127.0.0.1:29077
spark.jars                       file://$GLUTEN_JAR
spark.driver.extraClassPath      $GLUTEN_JAR
spark.executor.extraClassPath    $GLUTEN_JAR
spark.local.dir                  <nvme dirs>
spark.eventLog.dir               file://<dir>
```

The four keys without which Gluten cannot work are already in both files; check they survive any edits:
`spark.plugins=org.apache.gluten.GlutenPlugin`,
`spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager`,
`spark.memory.offHeap.enabled=true`, `spark.memory.offHeap.size=22g`.

## A. Best config

### A1. On Vera, with the existing binaries (fastest)

```
export JAVA_HOME=/home/nvidia/ferd/.deps/root/usr/lib/jvm/java-17-openjdk-arm64
export SPARK_HOME=/home/nvidia/ferd/.deps/spark4-env/lib/python3.11/site-packages/pyspark   # Spark 4.0.1
export PYSPARK_PYTHON=/home/nvidia/ferd/.deps/spark4-env/bin/python
GLUTEN_JAR=/home/nvidia/ferd/benchmarks/vera-confsweep-20261106/pgo/gluten.jar             # sha256 6ba16c52...
```

Start master/workers as above, then submit `rtm/scripts/benchmark_scale.py <tpcds|tpch> <out_dir>` with:

```
TPC_DATA_ROOTS=/home/nvidia/ferd/benchmarks/baseline-alignment-20261029/views/tpcds/sf1000
TPC_EXECUTOR_COUNT=42 TPC_EXECUTOR_CORES=8 TPC_CACHE_POLICY=retain
TPC_QUERIES=            # empty = full suite, or e.g. query3,query10
TPC_QUERY_REPETITIONS=1 TPC_WARMUP=0 TPC_PERF_PROFILE=0
TPC_NATIVE_PACKAGE=<dir of gluten.jar>/package.json   # optional hash check of the native libs
```

`harness/run-suite-strict.py` + `run-monitored-strict.py` are the scripts that produced the AB numbers (cgroup
isolation, memory/swap gates, correctness compare). They hard-code Vera paths; use them on Vera only.

### A2. Build from source

The AB jar was assembled incrementally (base jar from `3673833c2`, then class/native-library replacement for
anti/existence build-left, stateless validation and the PGO libvelox), so a source build is functionally the
same but not byte-identical. `93c4221d3` already contains the build-left extensions in source form.

```
# Velox
git clone https://github.com/HighPerfDataAccelerator/velox && cd velox
git checkout be88416a3c551affec8c69c792cbeb441f03bb48
git apply <gluten>/dev/vera/patches/velox-join-build-window128.patch

# Gluten
git clone https://github.com/HighPerfDataAccelerator/gluten && cd gluten
git checkout 93c4221d3            # or 3673833c2, the exact base of the AB jar
git apply dev/vera/patches/gluten-stateless-validation.patch   # copy it out of this branch first
./dev/builddeps-veloxbe.sh --velox_home=<velox> --arm_build_target=local --build_type=Release \
    --enable_vcpkg=ON --spark_version=4.0
mvn -B -Pbackends-velox -Pspark-4.0 -Pscala-2.13 -Pjava-17 -DskipTests package
# -> package/target/gluten-velox-bundle-spark4.0_2.13-*.jar
```

Optional gcc PGO for libvelox (about -4%): `pgo/cfg.sh gen|use` configures Velox with
`-fprofile-generate` / `-fprofile-use`; run the suites once with the `gen` library to collect `.gcda`, rebuild
with `use`, relink (`pgo/link.py`) and swap `linux/aarch64/libvelox.so` in the jar (`pgo/pkg.py`). The scripts
contain Vera paths. Training used 32 NDS + 10 NDS-H queries.

## B. RTM pipelined shuffle (Spark 5.0)

```
# Spark 5.0.0-SNAPSHOT with the RTM patches
git clone -b rtm-vera-fixes https://github.com/winningsix/spark && cd spark
build/mvn -T 48 -B -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip -Dcyclonedx.skip=true -Phive install
mkdir -p ../dist/spark-5.0.0-rtm/jars && cp -r bin sbin conf ../dist/spark-5.0.0-rtm/ \
  && cp assembly/target/scala-2.13/jars/*.jar ../dist/spark-5.0.0-rtm/jars/
# PySpark for the driver script: python/ from the same checkout (PYTHONPATH=python:python/lib/py4j-*.zip)

# Gluten JVM against that Spark (needs the `install` above in ~/.m2)
git clone -b rtm-vera https://github.com/HighPerfDataAccelerator/gluten && cd gluten
mvn -B -Pbackends-velox -Pspark-4.2 -Pscala-2.13 -Pjava-17 -Dspark.version=5.0.0-SNAPSHOT \
    -DskipTests -Dspotless.check.skip -Dscalastyle.skip -Dcheckstyle.skip install
```

The `rtm-vera` build was only used for its JVM classes. The runnable jar is
`package/target/gluten-velox-bundle-spark4.2_2.13-*.jar` with every `linux/aarch64/*.so` entry replaced by the
ones from the A jar (sha256 `6ba16c52...`). Without that step the bundle has no matching native libraries.
Then run with `SPARK_HOME=dist/spark-5.0.0-rtm` and `conf/vera-rtm-on.conf`.

RTM-specific requirements (all in `vera-rtm-on.conf`; each one fixed a hang or an admission failure):
prepared receive service (`spark.shuffle.streaming.executorReceiveService.enabled`, `readerQueueMaxMemory`,
`sharedWriterServer.enabled`, `sharedConnections.enabled`), `spark.shuffle.streaming.groupAdmission.enabled`,
`writer/reader.waitForTerminationAcks=false`, `spark.shuffle.streaming.networkBufferSize=262144` and
`spark.gluten.sql.columnar.shuffle.pipelined.pushBufferSize=262144`. Use `rtm-vera-fixes`, not
`rtm-1p5x-20261105`: without its range-exchange rule, ORDER BY queries hang or fail with
"Range partitioning is not supported by pipelined shuffle". Verified queries: TPC-H Q1, Q3, Q6, Q10.

## Troubleshooting

### `java.lang.ClassCastException: ColumnarBatch cannot be cast to InternalRow`

A row-based Spark operator received Gluten columnar batches, i.e. a columnar-to-row transition is missing. In
order of likelihood:

1. **Jar / Spark mismatch.** Gluten picks its shim layer from the running Spark version. Check
   `spark-submit --version` and `unzip -p $GLUTEN_JAR gluten-build-info.properties` (`spark_version`, `branch`,
   `revision`). A: Spark 4.0.1 with a `spark4.0` bundle. B: the RTM 5.0.0-SNAPSHOT build with the
   `spark4.2` bundle from `rtm-vera`. Anything else is unsupported.
2. **Gluten not active on executors.** `spark.plugins`, `spark.shuffle.manager=...ColumnarShuffleManager` and
   `spark.executor.extraClassPath=$GLUTEN_JAR` must all be set. With only `spark.sql.extensions` the driver plans
   columnar operators that executors do not run.
3. **Two Gluten jars on the classpath** (for example a stock Gluten bundle in `$SPARK_HOME/jars` plus this one).
   Keep exactly one.
4. **RTM only**: a Spark built from `rtm-1p5x-20261105` instead of `rtm-vera-fixes`, or the `rtm-vera` bundle
   without the native libraries swapped in.

If none of these applies, send the full stack trace (the first `Caused by` and 30 frames), the query name, the
two version checks from step 1, and the conf in use.

### Other failures seen while bringing this up
- `CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT` (RTM): group admission / prepared receive service keys are missing.
- RTM job hangs, producers parked in `StreamingShuffleRawBufferPool.awaitBorrow`: `networkBufferSize` too large.
- `spark.task.cpus` < 1 with Gluten: needs the fractional parse in `SparkResourceUtil` (in `rtm-vera`).
