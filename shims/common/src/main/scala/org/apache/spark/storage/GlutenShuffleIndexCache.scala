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
package org.apache.spark.storage

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_USE_OLD_FETCH_PROTOCOL
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver

import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Executor-wide cache of shuffle index files for host-local block reads.
 *
 * Spark's IndexShuffleBlockResolver opens, reads and closes the map output's index file for every
 * block, so a reducer that reads M host-local map outputs pays M index opens, and every reducer on
 * the executor pays them again. An index file is immutable once committed and, with the new fetch
 * protocol, named by the unique map task attempt id, so its offsets can be read once and reused.
 *
 * Entries are grouped by shuffle. When the byte budget is exhausted, whole shuffles that were least
 * recently used are evicted; a shuffle that does not fit alone is served uncached rather than
 * thrashing.
 */
object GlutenShuffleIndexCache extends Logging {
  val ENABLED_KEY = "spark.gluten.sql.columnar.backend.velox.shuffle.indexCache.enabled"
  val MAX_BYTES_KEY = "spark.gluten.sql.columnar.backend.velox.shuffle.indexCache.maxBytes"
  private val DEFAULT_MAX_BYTES = 128L * 1024 * 1024

  private class ShuffleEntry {
    val offsets = new ConcurrentHashMap[String, Array[Long]]()
    val bytes = new AtomicLong(0)
    @volatile var lastAccess: Long = System.nanoTime()
  }

  private val shuffles = new ConcurrentHashMap[Int, ShuffleEntry]()
  private val totalBytes = new AtomicLong(0)

  @volatile private var settings: Option[(Boolean, Long, TransportConf)] = None

  private def currentSettings(conf: SparkConf): (Boolean, Long, TransportConf) = {
    settings.getOrElse {
      synchronized {
        settings.getOrElse {
          val enabled =
            conf.getBoolean(ENABLED_KEY, false) && !conf.get(SHUFFLE_USE_OLD_FETCH_PROTOCOL)
          val maxBytes = conf.getSizeAsBytes(MAX_BYTES_KEY, DEFAULT_MAX_BYTES)
          // FileSegmentManagedBuffer only consults the memory-map threshold and lazy file
          // descriptor settings, which do not depend on SSL options.
          val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
          val value = (enabled, maxBytes, transportConf)
          settings = Some(value)
          if (enabled) {
            logInfo(s"Shuffle index cache enabled with budget $maxBytes bytes")
          }
          value
        }
      }
    }
  }

  /** Same result as BlockManager.getHostLocalShuffleData, reusing cached index offsets. */
  def getHostLocalShuffleData(
      blockManager: BlockManager,
      blockId: BlockId,
      dirs: Array[String]): ManagedBuffer = {
    val (enabled, maxBytes, transportConf) = currentSettings(SparkEnv.get.conf)
    if (enabled) {
      getHostLocalShuffleData(blockManager, blockId, dirs, maxBytes, transportConf)
    } else {
      blockManager.getHostLocalShuffleData(blockId, dirs)
    }
  }

  private[storage] def getHostLocalShuffleData(
      blockManager: BlockManager,
      blockId: BlockId,
      dirs: Array[String],
      maxBytes: Long,
      transportConf: TransportConf): ManagedBuffer = {
    val range = blockId match {
      case id: ShuffleBlockId => Some((id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1))
      case id: ShuffleBlockBatchId =>
        Some((id.shuffleId, id.mapId, id.startReduceId, id.endReduceId))
      case _ => None
    }
    (SparkEnv.get.blockingShuffleManager.shuffleBlockResolver, range) match {
      case (resolver: IndexShuffleBlockResolver, Some((shuffleId, mapId, start, end))) =>
        val offsets = indexOffsets(resolver, shuffleId, mapId, dirs, maxBytes)
        if (end >= offsets.length || start > end) {
          // Let Spark report the malformed request with its usual error.
          blockManager.getHostLocalShuffleData(blockId, dirs)
        } else {
          new FileSegmentManagedBuffer(
            transportConf,
            resolver.getDataFile(shuffleId, mapId, Some(dirs)),
            offsets(start),
            offsets(end) - offsets(start))
        }
      case _ => blockManager.getHostLocalShuffleData(blockId, dirs)
    }
  }

  private[storage] def cachedBytes: Long = totalBytes.get()

  private[storage] def cachedShuffles: Set[Int] = {
    val ids = Set.newBuilder[Int]
    shuffles.keySet().forEach(id => ids += id)
    ids.result()
  }

  /** Drops the cached index offsets of an unregistered shuffle. */
  def removeShuffle(shuffleId: Int): Unit = synchronized {
    Option(shuffles.remove(shuffleId)).foreach(e => totalBytes.addAndGet(-e.bytes.get()))
  }

  private def indexOffsets(
      resolver: IndexShuffleBlockResolver,
      shuffleId: Int,
      mapId: Long,
      dirs: Array[String],
      maxBytes: Long): Array[Long] = {
    val indexFile = resolver.getIndexFile(shuffleId, mapId, Some(dirs))
    val key = indexFile.getPath
    val entry = shuffles.computeIfAbsent(shuffleId, _ => new ShuffleEntry)
    entry.lastAccess = System.nanoTime()
    val cached = entry.offsets.get(key)
    if (cached != null) {
      return cached
    }
    val bytes = Files.readAllBytes(indexFile.toPath)
    val offsets = new Array[Long](bytes.length / 8)
    ByteBuffer.wrap(bytes).asLongBuffer().get(offsets)
    insert(shuffleId, entry, key, offsets, bytes.length.toLong + 64, maxBytes)
    offsets
  }

  // Misses are rare next to hits (one per map output per executor), so insertion and eviction
  // share one lock to keep the byte accounting exact.
  private def insert(
      shuffleId: Int,
      entry: ShuffleEntry,
      key: String,
      offsets: Array[Long],
      size: Long,
      maxBytes: Long): Unit = synchronized {
    // Skip a shuffle that was unregistered or evicted since the lookup.
    if (shuffles.get(shuffleId) eq entry) {
      while (totalBytes.get() + size > maxBytes && evictOldestExcept(shuffleId)) {}
      if (totalBytes.get() + size <= maxBytes && entry.offsets.putIfAbsent(key, offsets) == null) {
        entry.bytes.addAndGet(size)
        totalBytes.addAndGet(size)
      }
    }
  }

  private def evictOldestExcept(shuffleId: Int): Boolean = {
    var oldest: Option[(Int, ShuffleEntry)] = None
    shuffles.forEach {
      (id, e) =>
        if (id != shuffleId && oldest.forall(_._2.lastAccess > e.lastAccess)) {
          oldest = Some((id, e))
        }
    }
    oldest.foreach { case (id, _) => removeShuffle(id) }
    oldest.isDefined
  }
}
