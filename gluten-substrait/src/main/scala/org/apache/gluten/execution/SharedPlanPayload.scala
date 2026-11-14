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
package org.apache.gluten.execution

import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * A task carries a small broadcast handle, while split metadata stays local. Existing
 * GlutenPartition and its public constructor/copy API are unchanged.
 */
final class SharedPlanGlutenPartition(
    override val index: Int,
    private val planReference: Broadcast[Array[Byte]],
    val splitInfos: Array[SplitInfo],
    val files: Array[String] = Array.empty[String]) extends BaseGlutenPartition {

  // Read-only contract, like the original GlutenPartition.plan. The native
  // consumer parses const bytes; never mutate the shared array or clone it per
  // task. Spark's Broadcast cache may refetch it after executor eviction.
  override def plan: Array[Byte] = planReference.value

  override def preferredLocations(): Array[String] =
    splitInfos.flatMap(_.preferredLocations().asScala)

  def broadcastId: Long = planReference.id
}

object SharedPlanPayload extends Logging {
  val enabledKey = "spark.gluten.experimental.sharedPlanPayload.enabled"
  val minBytesKey = "spark.gluten.experimental.sharedPlanPayload.minBytes"
  private val broadcasts = new java.util.concurrent.atomic.AtomicLong()
  private val sharedBytes = new java.util.concurrent.atomic.AtomicLong()
  private val sharedPartitions = new java.util.concurrent.atomic.AtomicLong()

  // Driver-side activation receipt; callers take deltas outside query timers.
  def snapshot(): java.util.Map[String, java.lang.Long] = {
    val result = new java.util.LinkedHashMap[String, java.lang.Long]()
    result.put("broadcasts", broadcasts.get())
    result.put("planBytes", sharedBytes.get())
    result.put("partitions", sharedPartitions.get())
    result
  }

  def partitions(
      context: SparkContext,
      planBytes: Array[Byte],
      splits: Seq[Array[SplitInfo]],
      enabled: Boolean,
      minBytes: Long): Seq[BaseGlutenPartition] = {
    require(
      minBytes >= 262144L && minBytes <= Int.MaxValue,
      "Shared plan threshold must be between 256 KiB and Int.MaxValue")
    if (!enabled || planBytes.length < minBytes || splits.isEmpty) {
      return splits.zipWithIndex.map {
        case (info, index) =>
          GlutenPartition(index, planBytes, info)
      }
    }
    require(context != null, "Large shared plans require the active SparkContext")
    // Snapshot once, so retaining/mutating the caller's array cannot modify an
    // already-published plan. The immutable payload is reused by all partitions.
    val reference = context.broadcast(planBytes.clone())
    broadcasts.incrementAndGet()
    sharedBytes.addAndGet(planBytes.length)
    sharedPartitions.addAndGet(splits.size)
    logInfo(s"SHARED_PLAN_PAYLOAD broadcastId=${reference.id} " +
      s"planBytes=${planBytes.length} partitions=${splits.size} minBytes=$minBytes")
    // Each cached RDD partition strongly owns its handle. No explicit destroy,
    // task-completion cleanup or TTL: retries and repeated RDD actions must keep
    // working. Spark's normal context cleaner reclaims an unreachable broadcast.
    splits.zipWithIndex.map {
      case (info, index) =>
        new SharedPlanGlutenPartition(index, reference, info)
    }
  }
}
