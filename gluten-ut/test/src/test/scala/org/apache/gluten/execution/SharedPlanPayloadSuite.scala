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

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}
import java.security.MessageDigest
import java.util.Arrays

object SharedPlanPayloadSuite {
  final case class TestSplit(label: String, hosts: java.util.List[String]) extends SplitInfo {
    override def preferredLocations(): java.util.List[String] = hosts

    override def toProtobuf(): com.google.protobuf.Message =
      throw new UnsupportedOperationException("This fixture does not invoke a native reader")
  }

  def serialize(value: AnyRef): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try {
      output.writeObject(value)
      output.flush()
      bytes.toByteArray
    } finally {
      output.close()
    }
  }

  def deserialize(bytes: Array[Byte]): FirstZippedPartitionsPartition = {
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes)) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        try {
          Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
        } catch {
          case _: ClassNotFoundException => super.resolveClass(desc)
        }
      }
    }
    try {
      input.readObject().asInstanceOf[FirstZippedPartitionsPartition]
    } finally {
      input.close()
    }
  }

  def digest(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString
}

class SharedPlanPayloadSuite extends AnyFunSuite with BeforeAndAfterAll {
  import SharedPlanPayloadSuite._

  private val threshold = 262144L
  private var context: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // This suite tests Spark transport only; no Gluten plugin or native backend is loaded.
    context = new SparkContext(
      new SparkConf(false)
        .setMaster("local[2]")
        .setAppName(getClass.getSimpleName)
        .set("spark.ui.enabled", "false"))
  }

  override protected def afterAll(): Unit = {
    try {
      if (context != null) {
        context.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  private def splitInfos: Seq[Array[SplitInfo]] = (0 until 4).map {
    i =>
      Array[SplitInfo](
        TestSplit(s"split-$i-a", Arrays.asList(s"host-$i")),
        TestSplit(s"split-$i-b", Arrays.asList("shared-host")))
  }

  test("disabled, small and empty plans keep the existing path without a context") {
    val payload = Array.fill[Byte](threshold.toInt)(3)
    val splits = splitInfos
    val before = SharedPlanPayload.snapshot().get("broadcasts")
    val disabled = SharedPlanPayload.partitions(null, payload, splits, false, threshold)
    assert(disabled.forall(p => p.isInstanceOf[GlutenPartition] && (p.plan eq payload)))
    val small = SharedPlanPayload.partitions(null, payload.dropRight(1), splits, true, threshold)
    assert(small.forall(_.isInstanceOf[GlutenPartition]))
    assert(SharedPlanPayload.partitions(null, payload, Seq.empty, true, threshold).isEmpty)
    assert(SharedPlanPayload.snapshot().get("broadcasts") == before)
  }

  test("reject invalid thresholds and a missing context for a shared plan") {
    val payload = Array.fill[Byte](threshold.toInt)(3)
    intercept[IllegalArgumentException] {
      SharedPlanPayload.partitions(context, payload, splitInfos, true, threshold - 1)
    }
    intercept[IllegalArgumentException] {
      SharedPlanPayload.partitions(context, payload, splitInfos, true, Int.MaxValue.toLong + 1)
    }
    intercept[IllegalArgumentException] {
      SharedPlanPayload.partitions(null, payload, splitInfos, true, threshold)
    }
  }

  test("the threshold boundary creates one broadcast and preserves partition metadata") {
    val payload = Array.fill[Byte](threshold.toInt)(7)
    val splits = splitInfos
    val before = SharedPlanPayload.snapshot()
    val shared = SharedPlanPayload.partitions(context, payload, splits, true, threshold)
      .map(_.asInstanceOf[SharedPlanGlutenPartition])
    val after = SharedPlanPayload.snapshot()
    assert(after.get("broadcasts") - before.get("broadcasts") == 1L)
    assert(after.get("planBytes") - before.get("planBytes") == payload.length.toLong)
    assert(after.get("partitions") - before.get("partitions") == splits.size.toLong)
    assert(shared.map(_.broadcastId).distinct.size == 1)
    shared.zipWithIndex.foreach {
      case (partition, index) =>
        assert(partition.index == index)
        assert(partition.splitInfos eq splits(index))
        assert(partition.files.isEmpty)
        assert(partition.preferredLocations().sameElements(Array(s"host-$index", "shared-host")))
        assert(partition.plan.sameElements(payload))
    }
  }

  test("caller mutation cannot change an already published plan") {
    val payload = Array.tabulate[Byte](threshold.toInt)(i => (i % 127).toByte)
    val expected = payload.clone()
    val shared = SharedPlanPayload.partitions(context, payload, splitInfos, true, threshold)
    payload(0) = 99
    payload(payload.length - 1) = 88
    assert(shared.forall(_.plan.sameElements(expected)))
  }

  test("task serialization carries a handle even after reading the shared bytes") {
    val payload = Array.tabulate[Byte](8 * 1024 * 1024)(i => (i % 127).toByte)
    val splits = splitInfos
    val original = FirstZippedPartitionsPartition(0, GlutenPartition(0, payload, splits.head))
    assert(serialize(original).length >= payload.length)
    val shared = SharedPlanPayload.partitions(context, payload, splits, true, threshold)
    shared.foreach {
      partition =>
        val encoded = serialize(FirstZippedPartitionsPartition(partition.index, partition))
        assert(encoded.length < 16384)
        val roundTrip = deserialize(encoded)
        val actual = roundTrip.inputPartition.asInstanceOf[SharedPlanGlutenPartition]
        assert(roundTrip.index == partition.index)
        assert(actual.index == partition.index)
        assert(actual.plan.sameElements(payload))
        assert(actual.preferredLocations().sameElements(partition.preferredLocations()))
        assert(serialize(roundTrip).length < 16384)
    }
  }

  test("repeated actions and recomputation retain the plan payload") {
    val payload = Array.tabulate[Byte](threshold.toInt)(i => (i % 127).toByte)
    val expected = digest(payload)
    val shared = SharedPlanPayload.partitions(context, payload, splitInfos, true, threshold)
    val encoded = shared.map(p => serialize(FirstZippedPartitionsPartition(p.index, p)))
    val rdd = context.parallelize(encoded, encoded.size).map {
      bytes =>
        val partition = deserialize(bytes).inputPartition.asInstanceOf[SharedPlanGlutenPartition]
        (partition.index, digest(partition.plan), partition.broadcastId)
    }.cache()
    try {
      val first = rdd.collect().sortBy(_._1)
      assert(first.length == encoded.size)
      assert(first.forall(_._2 == expected))
      assert(first.map(_._3).distinct.length == 1)
      assert(rdd.collect().sortBy(_._1).sameElements(first))
      rdd.unpersist(blocking = true)
      assert(rdd.collect().sortBy(_._1).sameElements(first))
    } finally {
      rdd.unpersist(blocking = true)
    }
  }
}
