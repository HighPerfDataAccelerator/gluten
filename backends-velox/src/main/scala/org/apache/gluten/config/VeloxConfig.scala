/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.config

import org.apache.spark.SparkEnv
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale
import java.util.concurrent.TimeUnit

/*
 * Note: Gluten configiguration.md is automatically generated from this code.
 * Make sure to run dev/gen-all-config-docs.sh after making changes to this file.
 */
class VeloxConfig(conf: SQLConf) extends GlutenConfig(conf) {
  import VeloxConfig._

  def reuseFilteredFinalAggregateEnabled: Boolean = getConf(REUSE_FILTERED_FINAL_AGGREGATE_ENABLED)

  def runtimeBloomCascadeEnabled: Boolean = getConf(RUNTIME_BLOOM_CASCADE_ENABLED)
  def runtimeBloomCascadeMaxCreationKeyBytes: Long =
    getConf(RUNTIME_BLOOM_CASCADE_MAX_CREATION_KEY_BYTES)
  def runtimeBloomCascadeMaxFilters: Int = getConf(RUNTIME_BLOOM_CASCADE_MAX_FILTERS)

  def runtimeBloomSelectiveKeysEnabled: Boolean = getConf(RUNTIME_BLOOM_SELECTIVE_KEYS_ENABLED)
  def runtimeBloomSelectiveKeysMaxCreationKeyBytes: Long =
    getConf(RUNTIME_BLOOM_SELECTIVE_KEYS_MAX_CREATION_KEY_BYTES)
  def runtimeBloomSelectiveKeysMaxFilters: Int = getConf(RUNTIME_BLOOM_SELECTIVE_KEYS_MAX_FILTERS)
  def runtimeBloomSemiJoinEnabled: Boolean = getConf(RUNTIME_BLOOM_SEMI_JOIN_ENABLED)
  def runtimeBloomSemiJoinMaxFilters: Int = getConf(RUNTIME_BLOOM_SEMI_JOIN_MAX_FILTERS)
  def dimensionBridgeEnabled: Boolean = getConf(DIMENSION_BRIDGE_ENABLED)
  def dimensionBridgeFactMinBytes: Long = getConf(DIMENSION_BRIDGE_FACT_MIN_BYTES)
  def dimensionBridgeDimensionMaxBytes: Long = getConf(DIMENSION_BRIDGE_DIMENSION_MAX_BYTES)
  def dimensionStringIdsEnabled: Boolean = getConf(DIMENSION_STRING_IDS_ENABLED)
  def dimensionStringIdsFactMinBytes: Long = getConf(DIMENSION_STRING_IDS_FACT_MIN_BYTES)
  def dimensionStringIdsDimensionMaxBytes: Long = getConf(DIMENSION_STRING_IDS_DIMENSION_MAX_BYTES)
  def bloomFilterBatchProbeEnabled: Boolean = getConf(BLOOM_FILTER_BATCH_PROBE_ENABLED)
  def armSveBloomProbeEnabled: Boolean = getConf(ARM_SVE_BLOOM_PROBE_ENABLED)

  def veloxSpillFileSystem: String = getConf(COLUMNAR_VELOX_SPILL_FILE_SYSTEM)

  def veloxResizeBatchesShuffleInput: Boolean =
    getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT)

  def veloxResizeBatchesShuffleOutput: Boolean =
    getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT)

  def enableHashShuffleReaderStreamMerge: Boolean =
    getConf(COLUMNAR_VELOX_HASH_SHUFFLE_READER_STREAM_MERGE_ENABLED)

  def enableVeloxResizeBatchesCopyRanges: Boolean =
    getConf(COLUMNAR_VELOX_RESIZE_BATCHES_COPY_RANGES_ENABLED)

  case class ResizeRange(min: Int, max: Int) {
    assert(max >= min)
    assert(min > 0, "Min batch size should be larger than 0")
    assert(max > 0, "Max batch size should be larger than 0")
  }

  def veloxResizeBatchesShuffleInputOutputRange: ResizeRange = {
    val standardSize = getConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE)
    val defaultMinSize: Int = (0.25 * standardSize).toInt.max(1)
    val minSize = getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_OUTPUT_MIN_SIZE)
      .getOrElse(defaultMinSize)
    ResizeRange(minSize, Int.MaxValue)
  }

  def castFromVarcharAddTrimNode: Boolean = getConf(CAST_FROM_VARCHAR_ADD_TRIM_NODE)

  def enableVeloxFlushablePartialAggregation: Boolean =
    getConf(VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED)

  def enableBroadcastBuildRelationInOffheap: Boolean =
    getConf(VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP)

  def broadcastBuildMergeBatches: Boolean =
    getConf(VELOX_BROADCAST_BUILD_MERGE_BATCHES)

  def enableBroadcastBuildOncePerExecutor: Boolean =
    getConf(VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR)

  def broadcastNestedLoopJoinFullOuterRewriteThreshold: Long =
    getConf(VELOX_BROADCAST_NESTED_LOOP_JOIN_FULL_OUTER_REWRITE_THRESHOLD)

  def veloxBroadcastHashTableBuildTargetBytes: Long =
    getConf(COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_TARGET_BYTES)

  def veloxOrcScanEnabled: Boolean =
    getConf(VELOX_ORC_SCAN_ENABLED)

  def scanMapKeyPruningEnabled: Boolean =
    getConf(SCAN_MAP_KEY_PRUNING_ENABLED)

  def floatingPointMode: String = getConf(FLOATING_POINT_MODE)

  def enableRewriteCastArrayToString: Boolean =
    getConf(ENABLE_REWRITE_CAST_ARRAY_TO_STRING)

  def enableRewriteUnboundedWindow: Boolean = getConf(ENABLE_REWRITE_UNBOUNDED_WINDOW)

  def enableEnhancedFeatures(): Boolean = ConfigJniWrapper.isEnhancedFeaturesEnabled &&
    getConf(ENABLE_ENHANCED_FEATURES)

  def veloxPreferredBatchBytes: Long = getConf(COLUMNAR_VELOX_PREFERRED_BATCH_BYTES)

  def enableRddScan: Boolean = getConf(COLUMNAR_VELOX_RDD_SCAN_ENABLED)

  def cudfEnableTableScan: Boolean = getConf(CUDF_ENABLE_TABLE_SCAN)

  def cudfEnableValidation: Boolean = getConf(CUDF_ENABLE_VALIDATION)

  def cudfBatchSize: Int = getConf(CUDF_BATCH_SIZE)

  def cudfShuffleMaxPrefetchBytes: Long = getConf(CUDF_SHUFFLE_MAX_PREFETCH_BYTES)

  def parquetUseColumnNames: Boolean = getConf(PARQUET_USE_COLUMN_NAMES)

  def parquetPageSizeBytes: Long = getConf(PARQUET_PAGE_SIZE_BYTES)

  def hashProbeBloomFilterPushdownMaxSize: Long = getConf(HASH_PROBE_BLOOM_FILTER_PUSHDOWN_MAX_SIZE)

  def hashProbeDynamicFilterPushdownEnabled: Boolean =
    getConf(HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED)

  def valueStreamDynamicFilterEnabled: Boolean =
    getConf(VALUE_STREAM_DYNAMIC_FILTER_ENABLED)

  def hashProbeBloomFilterBypassMinRows: Int = getConf(HASH_PROBE_BLOOM_FILTER_BYPASS_MIN_ROWS)

  def hashProbeBloomFilterBypassMinPct: Int = getConf(HASH_PROBE_BLOOM_FILTER_BYPASS_MIN_PCT)

  def scanBloomFilterPushdownEnabled: Boolean = getConf(SCAN_BLOOM_FILTER_PUSHDOWN_ENABLED)

  def enableTimestampNtzValidation: Boolean = getConf(ENABLE_TIMESTAMP_NTZ_VALIDATION)

  def enableDriverSideBroadcastHashTableBuild: Boolean =
    getConf(VELOX_DRIVER_SIDE_BROADCAST_HASH_TABLE_BUILD)

  def enableGpuAsyncShuffleReader: Boolean = getConf(ENABLE_GPU_ASYNC_SHUFFLE_READER)

  def gpuAsyncReaderMaxPrefetchBytes: Long = getConf(GPU_ASYNC_SHUFFLE_READER_MAX_PREFETCH_BYTES)
}

object VeloxConfig extends ConfigRegistry {
  val HASH_BUILD_BATCH_STORE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashBuild.batchStore.enabled")
      .doc("Store dense append-only hash build input one column at a time.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_BATCH_VALUE_IDS_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.batchValueIds.enabled")
      .doc("Reuse repeated key value IDs during hash join build, including deduplicating builds.")
      .booleanConf
      .createWithDefault(false)

  val GROUPING_SETS_BATCHING_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.groupingSetsBatching.enabled")
      .doc("Reuse column hashes across grouping-set projections of the same Expand input batch.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_ADAPTIVE_VALUE_IDS_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.adaptiveValueIds.enabled")
      .doc(
        "Sample append-only join builds and abandon high-cardinality value-ID dictionaries early.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_ADAPTIVE_VALUE_IDS_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.adaptiveValueIds.minRows")
      .doc("Minimum selected build rows before considering value-ID dictionary abandonment.")
      .intConf
      .checkValue(_ > 0, "Must be positive")
      .createWithDefault(32768)

  val HASH_JOIN_ADAPTIVE_VALUE_IDS_MIN_DISTINCT_PCT =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.adaptiveValueIds.minDistinctPct")
      .doc("Minimum distinct/build-row percentage for early dictionary abandonment.")
      .intConf
      .checkValue(v => v >= 1 && v <= 100, "Must be between 1 and 100")
      .createWithDefault(80)

  val SHUFFLE_LZ4_FRAME_FAST_COMPRESSION_LEVEL =
    buildStaticConf(
      "spark.gluten.sql.columnar.backend.velox.shuffle.lz4Frame.fastCompressionLevel")
      .doc("Magnitude of the native LZ4F negative compression level, favoring speed over ratio. Zero preserves defaults. Standard independent frames, codec and task memory budgets remain unchanged. Set before Spark starts.")
      .intConf
      .checkValue(v => v >= 0 && v <= 32, "Must be between0and32")
      .createWithDefault(0)

  val REUSE_FILTERED_FINAL_AGGREGATE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.reuseFilteredFinalAggregate.enabled")
      .doc("Materialize repeated deterministic key-only final aggregation and HAVING results for normal Spark exchange reuse. Preserves partitioning and uses ordinary shuffle budgets. A weak HAVING filter may add significant shuffle traffic; disabled by default.")
      .booleanConf
      .createWithDefault(false)

  val Q5_SECOND_HOP_BLOOM_ENABLED =
    buildConf("spark.gluten.experimental.q5SecondHopBloom.enabled")
      .experimental()
      .doc(
        "Enable the experimental Q5-shaped supplier-to-lineitem Bloom filter after the " +
          "existing runtime cascade. Requires Q5SecondHopBloomExtension and runtimeBloom.cascade " +
          "to be enabled. Uses the existing filter-count, creation-size and scan thresholds. " +
          "Default off; benchmark at the intended scale before enabling.")
      .booleanConf
      .createWithDefault(false)

  val RUNTIME_BLOOM_CASCADE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.cascade.enabled")
      .doc("Build bounded extra runtime Bloom filters from dimensions already selected by stock runtime filters. Int/long inner join keys only; disabled by default.")
      .booleanConf.createWithDefault(false)
  val RUNTIME_BLOOM_CASCADE_MAX_CREATION_KEY_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.cascade.maxCreationKeyBytes")
      .doc("Maximum estimated projected-key bytes for a cascading filter creation scan. Existing Spark Bloom state memory limits remain in force.")
      .longConf.checkValue(_ > 0, "Must be positive").createWithDefault(268435456L)
  val RUNTIME_BLOOM_CASCADE_MAX_FILTERS =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.cascade.maxFilters")
      .doc("Maximum additional filters in a single cascade wave per query, between zero and ten.")
      .intConf.checkValue(
        v => v >= 0 && v <= 10,
        "Must be between zero and ten").createWithDefault(2)

  val RUNTIME_BLOOM_SELECTIVE_KEYS_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.selectiveKeys.enabled")
      .doc("Build bounded key-domain Bloom filters from flat dimensions with at least two distinct literal equality predicates; use Spark constraint inference to prune equivalent aggregate grouping keys. Disabled by default.")
      .booleanConf.createWithDefault(false)
  val RUNTIME_BLOOM_SELECTIVE_KEYS_MAX_CREATION_KEY_BYTES =
    buildConf(
      "spark.gluten.sql.columnar.backend.velox.runtimeBloom.selectiveKeys.maxCreationKeyBytes")
      .doc("Maximum estimated projected-key bytes for a selective-dimension creation scan. Existing Spark Bloom state memory limits remain in force.")
      .longConf.checkValue(_ > 0, "Must be positive").createWithDefault(2147483648L)
  val RUNTIME_BLOOM_SELECTIVE_KEYS_MAX_FILTERS =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.selectiveKeys.maxFilters")
      .doc(
        "Maximum additional filters in a single selective-key wave per query, between zero and ten.")
      .intConf.checkValue(
        v => v >= 0 && v <= 10,
        "Must be between zero and ten").createWithDefault(2)
  val RUNTIME_BLOOM_SEMI_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.semiJoin.enabled")
      .doc("Build a Bloom filter from the runtime-filtered retained side of a left semi/anti equijoin and prune its existence side through inner equijoins and grouping keys. Disabled by default.")
      .booleanConf.createWithDefault(false)
  val RUNTIME_BLOOM_SEMI_JOIN_MAX_FILTERS =
    buildConf("spark.gluten.sql.columnar.backend.velox.runtimeBloom.semiJoin.maxFilters")
      .doc("Maximum semi/anti existence-side filters per query, between zero and ten.")
      .intConf.checkValue(
        v => v >= 0 && v <= 10,
        "Must be between zero and ten").createWithDefault(2)
  val DIMENSION_BRIDGE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.joinReorder.dimensionBridge.enabled")
      .doc("Join two large inner-join inputs with their small bridging dimensions first when the dimensions are related by an extra equality, so that equality becomes part of the large join key. Disabled by default.")
      .booleanConf.createWithDefault(false)
  val DIMENSION_BRIDGE_FACT_MIN_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.joinReorder.dimensionBridge.factMinBytes")
      .doc("Minimum scanned bytes for an input to be treated as a large join input.")
      .longConf.checkValue(_ >= 0, "Must not be negative").createWithDefault(1073741824L)
  val DIMENSION_BRIDGE_DIMENSION_MAX_BYTES =
    buildConf(
      "spark.gluten.sql.columnar.backend.velox.joinReorder.dimensionBridge.dimensionMaxBytes")
      .doc("Maximum scanned bytes for an input to be treated as a bridging dimension.")
      .longConf.checkValue(_ >= 0, "Must not be negative").createWithDefault(67108864L)
  val DIMENSION_STRING_IDS_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.dimensionStringIds.enabled")
      .doc("Group large aggregations by injective integer identifiers of small-dimension string keys and restore the strings after aggregation. Disabled by default.")
      .booleanConf.createWithDefault(false)
  val DIMENSION_STRING_IDS_FACT_MIN_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.dimensionStringIds.factMinBytes")
      .doc("Minimum scanned bytes below an aggregation before string keys are encoded.")
      .longConf.checkValue(_ >= 0, "Must not be negative").createWithDefault(68719476736L)
  val DIMENSION_STRING_IDS_DIMENSION_MAX_BYTES =
    buildConf(
      "spark.gluten.sql.columnar.backend.velox.aggregation.dimensionStringIds.dimensionMaxBytes")
      .doc("Maximum scanned bytes of a dimension whose string keys are encoded.")
      .longConf.checkValue(_ >= 0, "Must not be negative").createWithDefault(67108864L)

  val BLOOM_FILTER_BATCH_PROBE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.batchProbe.enabled")
      .doc("Evaluate might_contain with a batched native probe that hashes and prefetches blocks of rows. Results are unchanged. Disabled by default.")
      .booleanConf.createWithDefault(false)
  val ARM_SVE_BLOOM_PROBE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.arm.sveBloomProbe.enabled")
      .doc("With bloomFilter.batchProbe.enabled, use ARM SVE vector hashing and gathers for the batched might_contain probe. Results are unchanged. Disabled by default.")
      .booleanConf.createWithDefault(false)

  val SHUFFLE_INDEX_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.shuffle.indexCache.enabled")
      .doc("Cache shuffle index file offsets per executor for host-local block reads instead of reopening the index file for every block. Ignored with spark.shuffle.useOldFetchProtocol. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_INDEX_CACHE_MAX_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.shuffle.indexCache.maxBytes")
      .doc("Executor heap budget of the shuffle index cache. Least recently used shuffles are evicted as a whole. Set before Spark starts.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("128MB")

  val ROW_CONTAINER_BATCH_STRING_EXTRACTION_ENABLED =
    buildStaticConf(
      "spark.gluten.sql.columnar.backend.velox.rowContainer.batchStringExtraction.enabled")
      .doc("Batch RowContainer string output into output-owned buffers. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ROW_CONTAINER_STRING_EXTRACTION_PREFETCH_ENABLED =
    buildStaticConf(
      "spark.gluten.sql.columnar.backend.velox.rowContainer.stringExtractionPrefetch.enabled")
      .doc("Prefetch row metadata and string payload headers in batched extraction. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val HASH_SHUFFLE_SVE_GATHER_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashShuffle.sveGather.enabled")
      .doc("Use native SVE gather for fixed-width hash shuffle columns on SVE builds.")
      .booleanConf
      .createWithDefault(true)

  val ARM_SVE_GATHER_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.arm.sveGather.enabled")
      .doc("Use predicated ARM SVE gathers with the existing NEON vector ABI. " +
        "Requires an SVE-enabled native build. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ROW_CONTAINER_BATCH_FIXED_EXTRACTION_ENABLED =
    buildStaticConf(
      "spark.gluten.sql.columnar.backend.velox.rowContainer.batchFixedExtraction.enabled")
      .doc("Batch integer row extraction and null bitmap updates. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ARM_SVE_ROW_EXTRACTION_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.arm.sveRowExtraction.enabled")
      .doc("Use predicated ARM SVE gathers for direct non-nullable integer row extraction. " +
        "Preserves the NEON ABI. Other paths need the independent batch switch. " +
        "Requires an SVE-enabled native build. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ARM_SVE_HASH_TAIL_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.arm.sveHashTail.enabled")
      .doc("Read bounded CRC32C string-hash tails with predicated ARM SVE loads. " +
        "Preserves hash values and falls back on other builds. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ARM_SVE_BIT_DEPOSIT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.arm.sveBitDeposit.enabled")
      .doc("Decode contiguous selected bit-packed integers with ARM SVE2 BitPerm. " +
        "Requires a compatible native build and CPU. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ARM_SVE_BIT_UNPACK_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.arm.sveBitUnpack.enabled")
      .doc("Decode selected bit-packed integer values with predicated ARM SVE. " +
        "Requires an SVE-enabled native build. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ROW_CONTAINER_STRING_EQUALITY_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.rowContainer.stringEquality.enabled")
      .doc("Use equality-only string key comparison. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_COMPACT_CHAINS_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.hashJoin.compactChains.enabled")
      .doc("Use bounded immutable posting lists for long join chains. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_COMPACT_CHAINS_MAX_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.hashJoin.compactChains.maxBytes")
      .doc("Maximum posting list/index bytes per table; zero disables optional allocation.")
      .longConf
      .checkValue(v => v >= 0 && v <= 536870912L, "Must be between 0 and 512 MiB")
      .createWithDefault(134217728L)

  val HASH_JOIN_ESTIMATED_WIDTH_OUTPUT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.hashJoin.estimatedWidthOutput.enabled")
      .doc(
        "Skip per-row accounting when the existing row-size estimate proves a row-limited batch.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_HEAD_CHAIN_DRAIN_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.hashJoin.headChainDrain.enabled")
      .doc("Drain long duplicate chains after later chains are buffered. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val HASH_JOIN_FIXED_WIDTH_OUTPUT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.hashJoin.fixedWidthOutput.enabled")
      .doc("Skip per-hit byte accounting for row-limited fixed-width join output. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_ARROW_MEMORY_MANAGER_REUSE_ENABLED =
    buildStaticConf(
      "spark.gluten.sql.columnar.backend.velox.shuffle.arrowMemoryManagerReuse.enabled")
      .doc("Reuse a CPU MemoryManager per Gluten Arrow pool. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_BYTE_LZ4_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.shuffle.byteShuffleLz4.enabled")
      .doc("Selectively reorder bytes before LZ4 shuffle compression. Requires matching new readers. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val ARM_NEON_BYTE_SHUFFLE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.arm.neonByteShuffle.enabled")
      .doc(
        "Use ARM NEON for optional lossless byte-plane shuffle transforms. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_BYTE_LZ4_LOG_USAGE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.shuffle.byteShuffleLz4.logUsage")
      .doc(
        "Diagnostic only: log one tagged shuffle encode/decode per thread. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_LZ4_FRAME_CONTEXT_REUSE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.shuffle.lz4FrameContextReuse.enabled")
      .doc("Reuse thread-local LZ4 scratch context for independent frames; requires LZ4 1.10. Set before Spark starts.")
      .booleanConf
      .createWithDefault(false)

  val GROUPING_SETS_VALUE_IDS_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.groupingSetsValueIds.enabled")
      .doc("Larger string dictionaries only for Expand-fed partial aggregation.")
      .booleanConf
      .createWithDefault(false)

  val GROUPING_SETS_VALUE_IDS_REUSE_ENABLED =
    buildConf(
      "spark.gluten.sql.columnar.backend.velox.aggregation.groupingSetsValueIds.reuse.enabled")
      .doc("Reuse exact string value IDs across projections within one Expand input batch.")
      .booleanConf
      .createWithDefault(true)

  val GROUPING_SETS_VALUE_IDS_MAX_STRING_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.groupingSetsValueIds.maxDistinctStringBytes")
      .doc("String dictionary byte threshold for eligible partial aggregation only.")
      .longConf
      .checkValue(
        v => v >= (1L << 20) && v <= (64L << 20),
        "Outside supported grouping dictionary range")
      .createWithDefault(33554432L)

  val GROUPING_SETS_VALUE_IDS_MAX_STRING_VALUES =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.groupingSetsValueIds.maxDistinctStringValues")
      .doc("String dictionary value limit for eligible partial aggregation only.")
      .longConf
      .checkValue(v => v >= 100000L && v <= 1000000L, "Outside supported grouping dictionary range")
      .createWithDefault(500000L)

  val HASH_JOIN_NUM_PARALLEL_CHAINS =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.numParallelChains")
      .doc("Number of interleaved duplicate-key join chains. The default preserves eight chains.")
      .intConf
      .checkValue(v => v >= 1 && v <= 32, "Must be between 1 and 32")
      .createWithDefault(8)

  val HASH_JOIN_AHEAD_WINDOW =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.aheadWindow")
      .doc("Maximum buffered results per interleaved duplicate-key join chain.")
      .intConf
      .checkValue(v => v >= 1 && v <= 32, "Must be between 1 and 32")
      .createWithDefault(8)

  val HASH_JOIN_PREFETCH_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashJoin.prefetch.enabled")
      .doc("Prefetch rows and next pointers during duplicate-key join enumeration.")
      .booleanConf
      .createWithDefault(true)

  override def get: VeloxConfig = {
    new VeloxConfig(SQLConf.get)
  }

  /**
   * Reads the flag straight off the SparkConf instead of going through [[get]].
   *
   * Session extensions are applied while the SparkSession is still being built, so `SQLConf.get`
   * returns defaults at that point and [[get]] would report this flag as off however the user set
   * it.
   */
  def nativeUDFBypassRegistration: Boolean =
    Option(SparkEnv.get).exists(_.conf.get(NATIVE_UDF_BYPASS_REGISTRATION))

  // velox caching options.
  val COLUMNAR_VELOX_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cacheEnabled")
      .doc(
        "Enable Velox cache, default off. It's recommended to enable" +
          "soft-affinity as well when enable velox cache.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEM_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.memCacheSize")
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_MEM_INIT_CAPACITY =
    buildConf("spark.gluten.sql.columnar.backend.velox.memInitCapacity")
      .doc("The initial memory capacity to reserve for a newly created Velox query memory pool.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  val COLUMNAR_VELOX_MEM_RECLAIM_MAX_WAIT_MS =
    buildConf("spark.gluten.sql.columnar.backend.velox.reclaimMaxWaitMs")
      .doc("The max time in ms to wait for memory reclaim.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(60))

  val COLUMNAR_VELOX_MEMORY_POOL_CAPACITY_TRANSFER_ACROSS_TASKS =
    buildConf("spark.gluten.sql.columnar.backend.velox.memoryPoolCapacityTransferAcrossTasks")
      .doc("Whether to allow memory capacity transfer between memory pools from different tasks.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_SSD_CACHE_PATH =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCachePath")
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_VELOX_SSD_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheSize")
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_SHARDS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheShards")
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_CACHE_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads")
      .doc("The number of IO threads for SSD cache read/write operations")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(4)

  val COLUMNAR_VELOX_SSD_ODIRECT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdODirect")
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_DISABLE_FILE_COW =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdDisableFileCow")
      .doc("True if copy on write should be disabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_CHECKSUM_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdChecksumEnabled")
      .doc("If true, checksum write to SSD is enabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_CHECKSUM_READ_VERIFICATION_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdChecksumReadVerificationEnabled")
      .doc("If true, checksum read verification from SSD is enabled.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_SSD_CHCEKPOINT_INTERVAL_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCheckpointIntervalBytes")
      .doc(
        "Checkpoint after every 'checkpointIntervalBytes' for SSD cache. " +
          "0 means no checkpointing.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_CONNECTOR_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.IOThreads")
      .doc(
        "The Size of the IO thread pool in the Connector. " +
          "This thread pool is used for split preloading and DirectBufferedInput. " +
          "By default, the value is the same as the maximum task slots per Spark executor.")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_TARGET_BYTES =
    buildStaticConf("spark.gluten.velox.broadcast.build.targetBytesPerThread")
      .doc(
        "It is used to calculate the number of hash table build threads. Based on our testing" +
          " across various thresholds (1MB to 128MB), we recommend a value of 32MB or 64MB," +
          " as these consistently provided the most significant performance gains.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32MB")

  val VELOX_BROADCAST_NESTED_LOOP_JOIN_FULL_OUTER_REWRITE_THRESHOLD =
    buildConf(
      "spark.gluten.sql.columnar.backend.velox.broadcastNLJ.fullOuterRewriteThreshold")
      .doc(
        "Maximum per-side plan size in bytes for rewriting a full outer broadcast nested loop " +
          "join into a left outer join and an existence join followed by union. The rewrite is " +
          "applied only when both sides have known statistics and each side is at or below this " +
          "threshold. Set to -1 to disable the rewrite.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10MB")

  val COLUMNAR_VELOX_ASYNC_TIMEOUT_ON_TASK_STOPPING =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping")
      .doc("Timeout in milliseconds when waiting for runtime-scoped async work to finish during" +
        " teardown.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(30000L)

  val COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER =
    buildConf("spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver")
      .doc("The split preload per task")
      .intConf
      .createWithDefault(2)

  val SCAN_MAP_KEY_PRUNING_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.scanMapKeyPruningEnabled")
      .doc(
        "When true, a map column read by a native Parquet scan is declared to Velox with only " +
          "the constant keys the query looks up (m['k'], element_at(m, 'k'), null checks), so " +
          "the reader skips the other entries. Applies when every use of the map sits in the " +
          "filters and projections directly above the scan and the map is dropped before any " +
          "other operator sees it; otherwise the map is read whole. See ScanMapKeyPruning.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_GLOG_VERBOSE_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogVerboseLevel")
      .internal()
      .doc("Set glog verbose level in Velox backend, same as FLAGS_v.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_GLOG_SEVERITY_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel")
      .internal()
      .doc("Set glog severity level in Velox backend, same as FLAGS_minloglevel.")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SPILL_STRATEGY =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillStrategy")
      .doc("none: Disable spill on Velox backend; " +
        "auto: Let Spark memory manager manage Velox's spilling")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "auto"))
      .createWithDefault("auto")

  val COLUMNAR_VELOX_MAX_SPILL_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillLevel")
      .doc("The max allowed spilling level with zero being the initial spilling level")
      .intConf
      .createWithDefault(4)

  val COLUMNAR_VELOX_MAX_SPILL_FILE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillFileSize")
      .doc("The maximum size of a single spill file created")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SPILL_NUM_MAX_MERGE_FILES =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillNumMaxMergeFiles")
      .doc(
        "The max number of files to merge at a time when merging sorted files " +
          "into a single ordered stream. 0 means unlimited.")
      .intConf
      .checkValue(_ >= 0, "must be non-negative")
      .createWithDefault(0)

  val COLUMNAR_VELOX_SPILL_FILE_SYSTEM =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillFileSystem")
      .doc(
        "The filesystem used to store spill data. local: The local file system. " +
          "heap-over-local: Write file to JVM heap if having extra heap space. " +
          "Otherwise write to local file system.")
      .stringConf
      .checkValues(Set("local", "heap-over-local"))
      .createWithDefaultString("local")

  val COLUMNAR_VELOX_MAX_SPILL_RUN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillRunRows")
      .doc("The maximum row size of a single spill run")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("3M")

  val COLUMNAR_VELOX_MAX_SPILL_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillBytes")
      .doc("The maximum file size of a query")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("100G")

  val MAX_PARTITION_PER_WRITERS_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartitionsPerWritersSession")
      .doc("Maximum number of partitions per a single table writer instance.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(10000)

  val MAX_TARGET_FILE_SIZE_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.parquetMaxTargetFileSize")
      .doc(
        "The target file size for each output file when writing data. " +
          "0 means no limit on target file size, and the actual file size will be determined by " +
          "other factors such as max partition number and shuffle batch size.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ >= 0, "must be a non-negative number")
      .createWithDefault(0)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput")
      .doc(
        s"If true, combine small columnar batches together before sending to shuffle. " +
          s"The default minimum output batch size is equal to 0.25 * " +
          s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleOutput")
      .doc(
        s"If true, combine small columnar batches together right after shuffle read. " +
          s"The default minimum output batch size is equal to 0.25 * " +
          s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_HASH_SHUFFLE_READER_STREAM_MERGE_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashShuffle.reader.streamMerge.enabled")
      .doc(
        "Enables a reader-side raw payload merge fast path for plain hash shuffle payloads " +
          "within each shuffle input stream. This path merges payload buffers before Velox " +
          "vectors are materialized, so it has lower per-batch overhead than generic " +
          "VeloxResizeBatchesExec resizing, but it only covers plain payloads. Complex types " +
          "and dictionary-encoded payloads are not merged by this path. " +
          "VeloxResizeBatchesExec can still be enabled separately as a generic complement " +
          "for types and encodings not covered by this fast path. If false, each hash " +
          "shuffle payload is returned as its own columnar batch.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_RESIZE_BATCHES_COPY_RANGES_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.copyRanges.enabled")
      .doc(
        "Enables a VeloxResizeBatchesExec fast path that combines eligible batches using " +
          "Velox vector copyRanges instead of generic RowVector append. When possible, it " +
          "collects the small input batches for one VeloxResizeBatchesExec output, allocates " +
          "the output RowVector once, and bulk-copies child vector ranges. This is most useful " +
          "for shuffle-read outputs where plain hash shuffle payloads are materialized as " +
          "dense flat vectors. Complex vectors can also use copyRanges, but ARRAY and MAP " +
          "still rebuild nested offsets and sizes while bulk-copying child ranges. Unsupported " +
          "encodings such as dictionary and constant vectors fall back to the generic copy " +
          "path. This option is enabled by default and complements the reader-side raw " +
          "payload merge fast path: that path avoids materializing small plain payload " +
          "batches, while this option optimizes VeloxResizeBatchesExec when that operator " +
          "is enabled.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput.minSize")
      .doc(
        s"The minimum batch size for shuffle. If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. Only functions when " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_OUTPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInputOutput.minSize")
      .doc(
        s"The minimum batch size for shuffle input and output. " +
          s"If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. " +
          s"The same applies for batches output by shuffle read. " +
          s"Only functions when " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key} or " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .fallbackConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE)

  val COLUMNAR_VELOX_ENABLE_USER_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for user type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_SHOW_TASK_METRICS_WHEN_FINISHED =
    buildConf("spark.gluten.sql.columnar.backend.velox.showTaskMetricsWhenFinished")
      .doc("Show velox full task metrics when finished.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_TASK_METRICS_TO_EVENT_LOG_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.backend.velox.taskMetricsToEventLog.threshold")
      .internal()
      .doc("Sets the threshold in seconds for writing task statistics to the event log if the " +
        "task runs longer than this value. Configuring the value >=0 can enable the feature. " +
        "0 means all tasks report and save the metrics to eventlog. value <0 disable the feature.")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  val COLUMNAR_VELOX_MEMORY_USE_HUGE_PAGES =
    buildConf("spark.gluten.sql.columnar.backend.velox.memoryUseHugePages")
      .doc("Use explicit huge pages for Velox memory allocation.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_ENABLE_SYSTEM_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableSystemExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for system type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation")
      .doc(
        "Enable flushable aggregation. If true, Gluten will try converting regular aggregation " +
          "into Velox's flushable aggregation when applicable. A flushable aggregation could " +
          "emit intermediate result at anytime when memory is full / data reduction ratio is low."
      )
      .booleanConf
      .createWithDefault(true)

  val ADAPTIVE_PARTIAL_AGGREGATION_MEMORY_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.adaptivePartialMemory.enabled")
      .doc("Before a well-reducing partial aggregation flush, try reserving a larger buffer within the task budget. Explicit partial-memory caps take precedence.")
      .booleanConf
      .createWithDefault(false)

  val ADAPTIVE_PARTIAL_AGGREGATION_MEMORY_MAX_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.aggregation.adaptivePartialMemory.maxBytes")
      .doc("Maximum adaptive partial buffer in bytes, additionally capped at half the task offheap budget.")
      .longConf
      .checkValue(v => v >= 0 && v <= 1073741824L, "Must be between 0 and 1 GiB")
      .createWithDefault(1073741824L)

  val MAX_PARTIAL_AGGREGATION_MEMORY =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemory")
      .doc(
        "Set the max memory of partial aggregation in bytes. When this option is set to a " +
          "value greater than 0, it will override spark.gluten.sql.columnar.backend.velox." +
          "maxPartialAggregationMemoryRatio. Note: this option only works when flushable " +
          "partial aggregation is enabled. Ignored when spark.gluten.sql.columnar.backend." +
          "velox.flushablePartialAggregation=false."
      )
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val MAX_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio")
      .doc(
        "Set the max memory of partial aggregation as "
          + "maxPartialAggregationMemoryRatio of offheap size. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.1)

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemory")
      .doc(
        "Set the max extended memory of partial aggregation in bytes. When this option is set " +
          "to a value greater than 0, it will override spark.gluten.sql.columnar.backend.velox." +
          "maxExtendedPartialAggregationMemoryRatio. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemoryRatio")
      .doc(
        "Set the max extended memory of partial aggregation as "
          + "maxExtendedPartialAggregationMemoryRatio of offheap size. Note: this option only " +
          "works when flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.15)

  val ABANDON_PARTIAL_AGGREGATION_MIN_PCT =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct")
      .doc(
        "If partial aggregation aggregationPct greater than this value, "
          + "partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(90)

  val ABANDON_PARTIAL_AGGREGATION_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows")
      .doc(
        "If partial aggregation input rows number greater than this value, "
          + " partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(100000)

  val HASH_PROBE_BLOOM_FILTER_PUSHDOWN_MAX_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashProbe.bloomFilterPushdown.maxSize")
      .doc("The maximum byte size of Bloom filter that can be generated from hash probe. When " +
        "set to 0, no Bloom filter will be generated. To achieve optimal performance, this should" +
        " not be too larger than the CPU cache size on the host.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(0)

  val HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashProbe.dynamicFilterPushdown.enabled")
      .doc(
        "Whether hash probe can generate any dynamic filter (including Bloom filter) and push" +
          " down to upstream operators.")
      .booleanConf
      .createWithDefault(true)

  val VALUE_STREAM_DYNAMIC_FILTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.valueStream.dynamicFilter.enabled")
      .doc(
        "Whether to apply dynamic filters pushed down from hash probe in the ValueStream" +
          " (shuffle reader) operator to filter rows before they reach the hash join.")
      .booleanConf
      .createWithDefault(false)

  val HASH_PROBE_BLOOM_FILTER_BYPASS_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashProbe.bloomFilter.bypassMinRows")
      .doc(
        "Number of probe rows used to decide whether to bypass the build-side Bloom filter " +
          "for left outer, existence, and left anti joins.")
      .intConf
      .checkValue(_ >= 0, "The minimum number of rows must not be negative")
      .createWithDefault(0)

  val HASH_PROBE_BLOOM_FILTER_BYPASS_MIN_PCT =
    buildConf("spark.gluten.sql.columnar.backend.velox.hashProbe.bloomFilter.bypassMinPct")
      .doc(
        "Bypass the build-side Bloom filter when its acceptance percentage reaches this value.")
      .intConf
      .checkValue(value => value >= 0 && value <= 100, "The percentage must be in [0, 100]")
      .createWithDefault(85)

  val SCAN_BLOOM_FILTER_PUSHDOWN_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.scan.bloomFilterPushdown.enabled")
      .doc("Whether to push Bloom filters into Velox scans.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled")
      .doc(
        "Enables caching of open file handles to avoid repeated open/close overhead. " +
          "Benefits both local filesystems (fewer open/close syscalls and file descriptor " +
          "churn) and remote filesystems/object stores (reused connection state). Should be " +
          "disabled if files are mutable, i.e. file content may change while the file path " +
          "stays the same.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_NUM_CACHE_FILE_HANDLES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.numCacheFileHandles")
      .doc(
        "Maximum number of entries in the file handle cache. Each entry holds an open " +
          "file descriptor (local FS) or connection state (remote FS). Note that on " +
          "local filesystems, high values may approach the OS file descriptor limit " +
          "(ulimit -n). On remote object stores (S3, ABFS, GCS) entries represent " +
          "network connections/sockets rather than per-file OS file descriptors, but " +
          "they can still count toward OS resource limits (ulimit -n).")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(10000)

  val COLUMNAR_VELOX_FILE_HANDLE_EXPIRATION_DURATION_MS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleExpirationDurationMs")
      .doc(
        "Expiration time for cached file handles. Handles not accessed within this duration " +
          "are evicted from the cache. This prevents stale handles from accumulating (e.g., " +
          "expired HDFS leases, closed remote connections). Accepts a Spark duration string " +
          "(e.g., \"10m\", \"600s\") or a plain number interpreted as milliseconds. A value " +
          "of 0 disables TTL-based eviction.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ >= 0, "must be a non-negative number (0 disables TTL-based eviction)")
      .createWithDefaultString("10m")

  val DIRECTORY_SIZE_GUESS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.directorySizeGuess")
      .doc("Deprecated, rename to spark.gluten.sql.columnar.backend.velox.footerEstimatedSize")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FOOTER_ESTIMATED_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.footerEstimatedSize")
      .doc("Set the footer estimated size for velox file scan, " +
        "refer to Velox's footer-estimated-size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FILE_PRELOAD_THRESHOLD =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold")
      .doc("Set the file preload threshold for velox file scan, " +
        "refer to Velox's file-preload-threshold")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val PREFETCH_ROW_GROUPS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.prefetchRowGroups")
      .doc("Set the prefetch row groups for velox file scan")
      .intConf
      .createWithDefault(1)

  val LOAD_QUANTUM =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.loadQuantum")
      .doc("Set the load quantum for velox file scan, recommend to use the default value (256MB) " +
        "for performance consideration. If Velox cache is enabled, it can be 8MB at most.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256MB")

  val MAX_COALESCED_DISTANCE_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedDistance")
      .doc(" Set the max coalesced distance bytes for velox file scan")
      .stringConf
      .createWithDefaultString("512KB")

  val MAX_COALESCED_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes")
      .doc("Set the max coalesced bytes for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64MB")

  val CACHE_PREFETCH_MINPCT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct")
      .doc("Set prefetch cache min pct for velox file scan")
      .intConf
      .createWithDefault(0)

  val AWS_SDK_LOG_LEVEL =
    buildConf("spark.gluten.velox.awsSdkLogLevel")
      .internal()
      .doc("Log granularity of AWS C++ SDK in velox.")
      .stringConf
      .createWithDefault("FATAL")

  val AWS_S3_RETRY_MODE =
    buildConf("spark.gluten.velox.fs.s3a.retry.mode")
      .internal()
      .doc("Retry mode for AWS s3 connection error: legacy, standard and adaptive.")
      .stringConf
      .createWithDefault("legacy")

  val S3_UPLOAD_PART_ASYNC =
    buildStaticConf("spark.gluten.velox.s3UploadPartAsync")
      .doc("If true, S3 multipart upload parts are uploaded asynchronously.")
      .booleanConf
      .createWithDefault(false)

  val S3_MAX_CONCURRENT_UPLOAD_NUM =
    buildStaticConf("spark.gluten.velox.s3MaxConcurrentUploadNum")
      .doc("The maximum number of in-flight S3 part uploads per file.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(4)

  val S3_UPLOAD_THREADS =
    buildStaticConf("spark.gluten.velox.s3UploadThreads")
      .doc("The number of shared S3 part upload threads.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(16)

  val VELOX_ORC_SCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.orc.scan.enabled")
      .doc("Enable velox orc scan. If disabled, vanilla spark orc scan will be used.")
      .booleanConf
      .createWithDefault(true)

  val NATIVE_UDF_BYPASS_REGISTRATION =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.nativeUDF.bypassRegistration")
      .doc(
        "If true, a UDF from udfLibraryPaths can be called by the name it was registered " +
          "with, so you do not have to write a Java class for it or run CREATE TEMPORARY " +
          "FUNCTION. In exchange, there is no Java version to fall back to, so any query " +
          "Gluten cannot run natively will fail instead of running on Spark. Off by default.")
      .booleanConf
      .createWithDefault(false)

  val CAST_FROM_VARCHAR_ADD_TRIM_NODE =
    buildConf("spark.gluten.velox.castFromVarcharAddTrimNode")
      .doc(
        "If true, will add a trim node " +
          "which has the same semantic as vanilla Spark to CAST-from-varchar." +
          "Otherwise, do nothing.")
      .booleanConf
      .createWithDefault(false)

  val DECIMAL_TO_FLOAT_HIGH_PRECISION_CAST_ENABLED =
    buildConf("spark.gluten.velox.decimalToFloatHighPrecisionCastEnabled")
      .doc(
        "If true, enables high-precision casts from DECIMAL to REAL/DOUBLE in Velox, " +
          "which match vanilla Spark for values that cannot be represented exactly by " +
          "floating-point arithmetic. Disabled by default because it is slower than the " +
          "default conversion; enable it if precision matters more than throughput.")
      .booleanConf
      .createWithDefault(false)

  val VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP =
    buildConf("spark.gluten.velox.offHeapBroadcastBuildRelation.enabled")
      .experimental()
      .doc("Experimental: If enabled, broadcast build relation will use offheap memory. " +
        "Otherwise, broadcast build relation will use onheap memory.")
      .booleanConf
      .createWithDefault(false)

  val VELOX_BROADCAST_BUILD_MERGE_BATCHES =
    buildConf("spark.gluten.velox.broadcastBuild.mergeBatches")
      .doc(
        "If enabled, all columnar batches in a broadcast build relation will be " +
          "serialized into a single buffer to reduce the number of addInput calls in " +
          "HashBuild operator. This can significantly improve BHJ performance when " +
          "the broadcast table has many small batches, but may increase driver-side " +
          "peak memory and is not suitable for very large broadcasts.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_BATCH_SERIALIZER_COMPRESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.columnarBatchSerializerCompression")
      .internal()
      .doc("which compression for the columnar batch serializer (e.g. broadcast).")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "zstd", "zlib", "snappy", "lz4", "gzip"))
      .createWithDefault("none")

  val VELOX_HASHMAP_ABANDON_BUILD_DUPHASH_MIN_ROWS =
    buildConf("spark.gluten.velox.abandonDedupHashMap.minRows")
      .experimental()
      .doc("Experimental: abandon hashmap build if duplicated rows more than this number.")
      .intConf
      .createWithDefault(100000)

  val VELOX_MIN_TABLE_ROWS_FOR_PARALLEL_JOIN_BUILD =
    buildConf("spark.gluten.velox.minTableRowsForParallelJoinBuild")
      .experimental()
      .doc("Experimental: the minimum number of table rows that can trigger " +
        "the parallel hash join table build.")
      .intConf
      .createWithDefault(1000)

  val VELOX_JOIN_BUILD_VECTOR_HASHER_MAX_NUM_DISTINCT =
    buildConf("spark.gluten.velox.joinBuildVectorHasherMaxNumDistinct")
      .experimental()
      .doc("Experimental: maximum number of distinct values to keep when " +
        "merging vector hashers in join HashBuild.")
      .intConf
      .createWithDefault(1000000)

  val VELOX_HASHMAP_ABANDON_BUILD_DUPHASH_MIN_PCT =
    buildConf("spark.gluten.velox.abandonDedupHashMap.minPct")
      .experimental()
      .doc(
        "Experimental: abandon hashmap build if duplicated rows are more than this percentile. " +
          "Value is integer based and range is [0, 100].")
      .intConf
      .createWithDefault(80)

  val VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR =
    buildConf("spark.gluten.velox.buildHashTableOncePerExecutor.enabled")
      .internal()
      .doc(
        "When enabled, the hash table is " +
          "constructed once per executor. If not enabled, " +
          "the hash table is rebuilt for each task.")
      .booleanConf
      .createWithDefault(true)

  val VELOX_DRIVER_SIDE_BROADCAST_HASH_TABLE_BUILD =
    buildConf("spark.gluten.sql.columnar.backend.velox.driverSideBroadcastHashTableBuild")
      .doc(
        "Enable driver-side broadcast hash table build. When enabled, the hash table is " +
          "built and serialized on the driver, then broadcast to executors. When disabled, " +
          "each executor builds its own hash table from the broadcast data.")
      .booleanConf
      .createWithDefault(false)

  val QUERY_TRACE_ENABLED = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceEnabled")
    .doc("Enable query tracing flag.")
    .booleanConf
    .createWithDefault(false)

  val QUERY_TRACE_DIR = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceDir")
    .internal()
    .doc("Base dir of a query to store tracing data.")
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_NODE_IDS = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceNodeIds")
    .internal()
    .doc("A comma-separated list of plan node ids whose input data will be traced. " +
      "Empty string if only want to trace the query metadata.")
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_MAX_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceMaxBytes")
      .internal()
      .doc("The max trace bytes limit. Tracing is disabled if zero.")
      .longConf
      .createWithDefault(0)

  val QUERY_TRACE_TASK_REG_EXP =
    buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceTaskRegExp")
      .internal()
      .doc("The regexp of traced task id. We only enable trace on a task if its id matches.")
      .stringConf
      .createWithDefault("")

  val OP_TRACE_DIRECTORY_CREATE_CONFIG =
    buildConf("spark.gluten.sql.columnar.backend.velox.opTraceDirectoryCreateConfig")
      .internal()
      .doc(
        "Config used to create operator trace directory. This config is provided to" +
          " underlying file system and the config is free form. The form should be " +
          "defined by the underlying file system.")
      .stringConf
      .createWithDefault("")

  val FLOATING_POINT_MODE =
    buildConf("spark.gluten.sql.columnar.backend.velox.floatingPointMode")
      .doc(
        "Config used to control the tolerance of floating point operations alignment with Spark. " +
          "When the mode is set to strict, flushing is disabled for sum(float/double)" +
          "and avg(float/double). When set to loose, flushing will be enabled.")
      .stringConf
      .checkValues(Set("loose", "strict"))
      .createWithDefault("loose")

  val COLUMNAR_VELOX_MEMORY_CHECK_USAGE_LEAK =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.checkUsageLeak")
      .doc("Enable check memory usage leak.")
      .booleanConf
      .createWithDefault(true)

  val CUDF_MEMORY_RESOURCE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.memoryResource")
      .doc("GPU RMM memory resource.")
      .stringConf
      .checkValues(Set("cuda", "pool", "async", "arena", "managed", "managed_pool"))
      .createWithDefault("async")

  val CUDF_MEMORY_PERCENT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.memoryPercent")
      .doc("The initial percent of GPU memory to allocate for memory resource for one thread.")
      .intConf
      .createWithDefault(50)

  val CUDF_ENABLE_TABLE_SCAN =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.enableTableScan")
      .doc("Enable cudf table scan")
      .booleanConf
      .createWithDefault(false)

  val CUDF_ENABLE_VALIDATION =
    buildConf("spark.gluten.sql.columnar.backend.velox.cudf.enableValidation")
      .doc(
        "Heuristics you can apply to validate a cuDF/GPU plan and only offload when " +
          "the entire stage can be fully and profitably executed on GPU")
      .booleanConf
      .createWithDefault(true)

  val CUDF_ALLOW_CPU_FALLBACK =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.allowCpuFallback")
      .doc("Allow cuDF to fall back to CPU execution for unsupported operators.")
      .booleanConf
      .createWithDefault(true)

  val CUDF_CONCURRENT_GPU_TASKS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks")
      .doc("The number of concurrent GPU tasks to run.")
      .intConf
      .createWithDefault(1)

  val CUDF_BATCH_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.cudf.batchSize")
      .doc("Cudf input batch size after shuffle reader")
      .intConf
      .createWithDefault(Integer.MAX_VALUE)

  val CUDF_SHUFFLE_MAX_PREFETCH_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.cudf.shuffleMaxPrefetchBytes")
      .doc("Maximum bytes to prefetch in CPU memory during GPU shuffle read while waiting" +
        " for GPU available.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1028MB")

  val MEMORY_DUMP_ON_EXIT =
    buildConf("spark.gluten.monitor.memoryDumpOnExit")
      .internal()
      .doc(
        "Whether to trigger native memory dump when executor exits. Currently it uses jemalloc" +
          " for memory profiling, so if you want to enable it, also need to  build gluten" +
          " with `--enable_jemalloc_stats=ON`.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_REWRITE_CAST_ARRAY_TO_STRING =
    buildConf("spark.gluten.sql.rewrite.castArrayToString")
      .doc(
        "When true, rewrite `cast(array as String)` to" +
          " `concat('[', array_join(array, ', ', null), ']')` to allow offloading to Velox.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_REWRITE_UNBOUNDED_WINDOW =
    buildConf("spark.gluten.sql.rewrite.unboundedWindow")
      .internal()
      .doc("When true, rewrite unbounded window to an equivalent aggregate join operation" +
        " to avoid OOM.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_ENHANCED_FEATURES =
    buildConf("spark.gluten.sql.enable.enhancedFeatures")
      .doc("Enable some features including iceberg native write and other features.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_PREFERRED_BATCH_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.preferredBatchBytes")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("10MB")

  val COLUMNAR_VELOX_RDD_SCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.rddScan.enabled")
      .doc(
        "When true, offload RDDScanExec to Velox by converting the RDD[InternalRow] into" +
          " columnar batches through the native row-to-columnar path. Schemas that are not" +
          " supported by the Arrow export path (e.g. map or interval types) fall back to" +
          " vanilla Spark.")
      .booleanConf
      .createWithDefault(true)

  val VELOX_MAX_COMPILED_REGEXES =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxCompiledRegexes")
      .doc(
        "Controls maximum number of compiled regular expression patterns per function " +
          "instance per thread of execution.")
      .intConf
      .createWithDefault(100)

  val PARQUET_USE_COLUMN_NAMES =
    buildConf(GlutenConfig.VELOX_PARQUET_USE_COLUMN_NAMES)
      .doc("Maps table field names to file field names using names, not indices for Parquet files.")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_PAGE_SIZE_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.parquet.pageSizeBytes")
      .doc("The page size in bytes is for compression.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val PARQUET_DICT_SIZE_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.parquet.dictionaryPageSizeBytes")
      .doc("The maximum size in bytes for a Parquet dictionary page")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("2MB")

  val ENABLE_TIMESTAMP_NTZ_VALIDATION =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation")
      .doc(
        "Enable validation fallback for TimestampNTZ type. When true, plans with " +
          "TimestampNTZ in their input or output schemas fall back to Spark execution. " +
          "When false, supported TimestampNTZ operations are eligible for native execution, " +
          "subject to other validation rules.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_GPU_ASYNC_SHUFFLE_READER =
    buildConf("spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.enabled")
      .doc(
        "Experimental: Enable GPU async shuffle reader. " +
          "When true, the gpu shuffle reader will use a thread pool " +
          "to read and deserialize the input streams. " +
          "When false, the shuffle reader will execute in the current thread.")
      .booleanConf
      .createWithDefault(false)

  val GPU_ASYNC_SHUFFLE_READER_THREAD_POOL_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.threadPoolSize")
      .doc(
        "The number of threads used by GPU async shuffle reader for decompressing " +
          "and deserializing input streams.")
      .intConf
      .checkValue(_ > 0, "The thread pool size must be greater than 0.")
      .createWithDefault(1)

  val GPU_ASYNC_SHUFFLE_READER_MAX_PREFETCH_BYTES =
    buildConf(
      "spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.maxPrefetchBytes")
      .doc(
        "The maximum number of bytes to prefetch in CPU memory during GPU async shuffle read.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ > 0, "The max prefetch bytes must be greater than 0.")
      .createWithDefaultString("1GB")
}
