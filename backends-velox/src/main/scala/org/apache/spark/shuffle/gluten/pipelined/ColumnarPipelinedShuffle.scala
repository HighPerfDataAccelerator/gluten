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
package org.apache.spark.shuffle.gluten.pipelined

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{GlutenConfig, ShuffleWriterType}
import org.apache.gluten.execution.StageExecutionMode
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.{ColumnarBatchSerializerInstance, NativePartitioning, ShuffleWriterJniWrapper, UnifflePartitionWriterJniWrapper}

import org.apache.spark.{PipelinedShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.shuffle.{GlutenShuffleUtils, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.streaming.StreamingShuffleSerializerInstance
import org.apache.spark.shuffle.writer.PartitionPusher
import org.apache.spark.sql.execution.PipelinedColumnarRead
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockId
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.util.{CompletionIterator, SparkResourceUtil}

import io.netty.buffer.ByteBuf

import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util

import scala.reflect.ClassTag

/**
 * A pipelined (RTM streaming) shuffle dependency whose map side runs Gluten's native hash shuffle
 * writer in push mode. Each payload the native writer evicts is a self-contained block in the
 * same format the native shuffle reader consumes, so it travels as one opaque record
 * `(partitionId, bytes)` through the streaming transport. The reader feeds the received blocks,
 * in arrival order, as a lazy stream sequence into one native reader per task.
 */
class ColumnarPipelinedShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    partitioner: org.apache.spark.Partitioner,
    shuffleWriterProcessor: ShuffleWriteProcessor,
    val nativePartitioning: NativePartitioning,
    val metrics: Map[String, SQLMetric],
    val shuffleWriterType: ShuffleWriterType,
    val columnarSerializer: Serializer)
  extends PipelinedShuffleDependency[K, V, C](
    _rdd,
    partitioner,
    PipelinedPayloadSerializer,
    shuffleWriterProcessor = shuffleWriterProcessor)
  with PipelinedColumnarRead {

  override def readPipelined(
      context: TaskContext,
      startPartition: Int,
      endPartition: Int,
      readMetrics: ShuffleReadMetricsReporter,
      executionMode: StageExecutionMode): Iterator[ColumnarBatch] = {
    val reader = SparkEnv.get
      .shuffleManagerFor(this)
      .getReader[Any, Any](
        shuffleHandle,
        0,
        Int.MaxValue,
        startPartition,
        endPartition,
        context,
        readMetrics)
    val streams: Iterator[(BlockId, InputStream)] = reader.read().map {
      record =>
        val bytes = record._2.asInstanceOf[Array[Byte]]
        readMetrics.incRemoteBytesRead(bytes.length)
        (null, new ByteArrayInputStream(bytes): InputStream)
    }
    val deserializer = columnarSerializer
      .newInstance()
      .asInstanceOf[ColumnarBatchSerializerInstance]
      .deserializeStreams(streams, () => (), executionMode)
    val batches = deserializer.asKeyValueIterator.map {
      kv =>
        val batch = kv._2.asInstanceOf[ColumnarBatch]
        readMetrics.incRecordsRead(batch.numRows())
        batch
    }
    CompletionIterator[ColumnarBatch, Iterator[ColumnarBatch]](
      batches,
      context.taskMetrics().mergeShuffleReadMetrics())
  }
}

/**
 * Carries opaque native payloads. Each record value is an `Array[Byte]` holding one complete
 * native block; it is framed with a 4-byte length so several blocks can share one network buffer.
 * The stream format and the ByteBuf fast path are identical, as the streaming transport requires.
 */
object PipelinedPayloadSerializer extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new PipelinedPayloadSerializerInstance
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class PipelinedPayloadSerializerInstance
  extends SerializerInstance
  with StreamingShuffleSerializerInstance {

  override def writeValueToByteBuf(value: Any, output: ByteBuf): Unit = {
    val bytes = value.asInstanceOf[Array[Byte]]
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  override def keyValueIteratorFromByteBuf(input: ByteBuf): Iterator[(Any, Any)] =
    new Iterator[(Any, Any)] {
      override def hasNext: Boolean = input.isReadable
      override def next(): (Any, Any) = {
        val length = input.readInt()
        val bytes = new Array[Byte](length)
        input.readBytes(bytes)
        (null, bytes)
      }
    }

  override def serializeStream(s: OutputStream): SerializationStream = new SerializationStream {
    private val out = new DataOutputStream(s)
    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      val bytes = t.asInstanceOf[Array[Byte]]
      out.writeInt(bytes.length)
      out.write(bytes)
      this
    }
    override def writeKey[T: ClassTag](key: T): SerializationStream = this
    override def flush(): Unit = out.flush()
    override def close(): Unit = out.close()
  }

  override def deserializeStream(s: InputStream): DeserializationStream =
    new DeserializationStream {
      private val in = new DataInputStream(s)
      override def readObject[T: ClassTag](): T = {
        val length = in.readInt() // throws EOFException at the end of the stream
        val bytes = new Array[Byte](length)
        in.readFully(bytes)
        bytes.asInstanceOf[T]
      }
      override def readKey[T: ClassTag](): T = null.asInstanceOf[T]
      override def close(): Unit = in.close()
    }

  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}

/**
 * Runs the native hash shuffle writer with a push-mode partition writer and hands its payloads to
 * the streaming shuffle writer obtained by the base processor.
 */
class PipelinedColumnarShuffleWriteProcessor(writeMetrics: Map[String, SQLMetric])
  extends ShuffleWriteProcessor {

  override protected def createMetricsReporter(
      context: TaskContext): ShuffleWriteMetricsReporter =
    new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, writeMetrics)

  override def write(
      inputs: Iterator[_],
      dep: org.apache.spark.ShuffleDependency[_, _, _],
      mapId: Long,
      mapIndex: Int,
      context: TaskContext): MapStatus = {
    val columnarDep = dep.asInstanceOf[ColumnarPipelinedShuffleDependency[_, _, _]]
    val payloads = new NativePayloadIterator(
      inputs.asInstanceOf[Iterator[Product2[Any, ColumnarBatch]]],
      columnarDep,
      context)
    try {
      super.write(payloads, dep, mapId, mapIndex, context)
    } finally {
      payloads.close()
    }
  }
}

/** Pulls batches into the native writer only when the queue of pushed payloads is empty. */
private class NativePayloadIterator(
    input: Iterator[Product2[Any, ColumnarBatch]],
    dep: ColumnarPipelinedShuffleDependency[_, _, _],
    context: TaskContext)
  extends Iterator[Product2[Int, Array[Byte]]]
  with Logging {

  private val conf: SparkConf = SparkEnv.get.conf
  private val runtime = Runtimes.contextInstance(
    BackendsApiManager.getBackendName,
    "PipelinedShuffleWriter")
  private val partitionWriterJni = UnifflePartitionWriterJniWrapper.create(runtime)
  private val shuffleWriterJni = ShuffleWriterJniWrapper.create(runtime)
  private val queue = new util.ArrayDeque[Product2[Int, Array[Byte]]]()
  private val pusher = new PartitionPusher(
    (partitionId: Int, buffer: Array[Byte], length: Int) => {
      queue.addLast((partitionId, util.Arrays.copyOf(buffer, length)))
      length
    })
  private val metrics = dep.metrics
  private var nativeWriter = -1L
  private var finished = false

  private def createWriter(): Unit = {
    val numPartitions = dep.nativePartitioning.getNumPartitions
    val (codec, codecBackend, level, compressionBufferSize) =
      if (conf.get(SHUFFLE_COMPRESS)) {
        val c = GlutenShuffleUtils.getCompressionCodec(conf)
        (
          c,
          GlutenConfig.get.columnarShuffleCodecBackend.orNull,
          GlutenShuffleUtils.getCompressionLevel(conf, c),
          GlutenShuffleUtils.getCompressionBufferSize(conf, c))
      } else {
        (null, null, 0, 0)
      }
    val pushBufferSize = conf.getSizeAsBytes(
      "spark.gluten.sql.columnar.shuffle.pipelined.pushBufferSize",
      "1m").toInt
    val partitionWriter = partitionWriterJni.createPartitionWriter(
      numPartitions,
      codec,
      codecBackend,
      level,
      compressionBufferSize,
      pushBufferSize,
      pushBufferSize.toLong,
      pusher)
    nativeWriter = shuffleWriterJni.createHashShuffleWriter(
      numPartitions,
      dep.nativePartitioning.getShortName,
      GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, context.partitionId()),
      GlutenConfig.get.maxBatchSize,
      GlutenConfig.get.columnarShuffleReallocThreshold,
      GlutenConfig.get.columnarShufflePartitionBufferEvictThreshold,
      partitionWriter,
      false)
    runtime
      .memoryManager()
      .addSpiller(new Spiller() {
        override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
          if (!Spiller.Phase.SPILL.equals(phase) || nativeWriter == -1L) {
            0L
          } else {
            shuffleWriterJni.reclaim(nativeWriter, size)
          }
        }
      })
  }

  private def availableOffHeapPerTask(): Long =
    SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)

  private def advance(): Unit = {
    while (queue.isEmpty && !finished) {
      if (input.hasNext) {
        val cb = input.next()._2
        if (cb.numRows() > 0 && cb.numCols() > 0) {
          if (nativeWriter == -1L) createWriter()
          val start = System.nanoTime()
          val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
          val bytes =
            shuffleWriterJni.write(nativeWriter, cb.numRows(), handle, availableOffHeapPerTask())
          metrics("dataSize") += bytes
          metrics("shuffleWallTime") += System.nanoTime() - start
          metrics("numInputRows") += cb.numRows()
          metrics("inputBatches") += 1
        }
      } else {
        if (nativeWriter != -1L) {
          val start = System.nanoTime()
          shuffleWriterJni.stop(nativeWriter) // pushes the remaining partition buffers
          metrics("shuffleWallTime") += System.nanoTime() - start
          close()
        }
        finished = true
      }
    }
  }

  override def hasNext: Boolean = {
    advance()
    !queue.isEmpty
  }

  override def next(): Product2[Int, Array[Byte]] = {
    if (!hasNext) throw new NoSuchElementException
    queue.pollFirst()
  }

  def close(): Unit = {
    if (nativeWriter != -1L) {
      shuffleWriterJni.close(nativeWriter)
      nativeWriter = -1L
    }
  }
}
