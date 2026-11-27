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
package org.apache.spark.shuffle.sort

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.network.shuffle.checksum.Cause
import org.apache.spark.storage.BlockManagerId

import scala.collection.mutable
import scala.io.Source
import scala.util.control.NonFatal

/** Optional IPv4 RDMA-CM stream transport for executor-served columnar shuffle blocks. */
final private[spark] class RdmaShuffleTransport(
    conf: SparkConf,
    localId: BlockManagerId,
    delegate: BlockStoreClient)
  extends BlockStoreClient
  with Logging {

  private val prefix = RdmaShuffleTransport.Prefix
  private val binary = conf.get(prefix + "executable", "gluten-rdma-relay")
  private class Relay(val process: Process, val port: Int)
  private val peers = mutable.Map.empty[(String, Int), Relay]
  private var closed = false

  RdmaShuffleTransport.validate(conf)

  private def rail(host: String): String = conf.get(prefix + "address." + host, host)

  private def start(mode: String, bind: String, port: Int, host: String, target: Int): Relay = {
    val process = new ProcessBuilder(binary, mode, bind, port.toString, host, target.toString)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
      .start()
    // The child publishes its kernel-assigned port only after listen() succeeds. Closing its
    // stdin (including when the executor dies) also tears down all live relay connections.
    val output = Source.fromInputStream(process.getInputStream)
    try {
      val bound = output.getLines().next().toInt
      require(bound > 0 && bound <= 65535, s"Invalid RDMA relay port $bound")
      logInfo(s"Columnar shuffle RDMA $mode listening on $bind:$bound for $host:$target")
      new Relay(process, bound)
    } catch {
      case NonFatal(e) =>
        process.destroyForcibly()
        throw new IllegalStateException(s"Cannot start RDMA $mode on $bind:$port", e)
    } finally {
      output.close()
    }
  }

  private def listen(): Relay =
    start("ingress", rail(localId.host), localId.port, localId.host, localId.port)

  private var ingress = listen()

  def ensureListening(): Unit = synchronized {
    require(!closed, "RDMA shuffle transport has stopped")
    if (!ingress.process.isAlive) ingress = listen()
  }

  private def peerPort(host: String, port: Int): Int = synchronized {
    ensureListening()
    val key = (host, port)
    val previous = peers.get(key)
    val relay = previous.filter(_.process.isAlive).getOrElse {
      // Spark caches clients by host and port. Its retries must address the same peer.
      val replacement = start("egress", "127.0.0.1", previous.fold(0)(_.port), rail(host), port)
      peers.update(key, replacement)
      replacement
    }
    relay.port
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      files: DownloadFileManager): Unit = {
    if (host == localId.host) {
      delegate.fetchBlocks(host, port, execId, blockIds, listener, files)
    } else {
      val portOnLoopback =
        try {
          peerPort(host, port)
        } catch {
          case NonFatal(e) =>
            blockIds.foreach(listener.onBlockFetchFailure(_, e))
            return
        }
      delegate.fetchBlocks("127.0.0.1", portOnLoopback, execId, blockIds, listener, files)
    }
  }

  override def diagnoseCorruption(
      host: String,
      port: Int,
      execId: String,
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      checksum: Long,
      algorithm: String): Cause =
    delegate.diagnoseCorruption(host, port, execId, shuffleId, mapId, reduceId, checksum, algorithm)

  override def close(): Unit = synchronized {
    closed = true
    val children = (peers.values.toList :+ ingress).map(_.process)
    children.foreach(_.destroy())
    peers.clear()
  }
}

private[spark] object RdmaShuffleTransport {
  val Prefix = "spark.gluten.shuffle.rdma."

  def validate(conf: SparkConf): Unit = {
    require(
      conf.getBoolean("spark.authenticate", false) &&
        conf.getBoolean("spark.network.crypto.enabled", false),
      "RDMA shuffle requires Spark authentication and network encryption"
    )
    require(
      !conf.getBoolean("spark.ssl.rpc.enabled", conf.getBoolean("spark.ssl.enabled", false)),
      "RDMA shuffle currently supports Spark network crypto with RPC TLS disabled"
    )
    Seq("spark.shuffle.service.enabled", "spark.shuffle.push.enabled").foreach {
      key => require(!conf.getBoolean(key, false), s"RDMA shuffle does not support $key")
    }
    val migrateShuffleBlocks = conf.getBoolean("spark.decommission.enabled", false) &&
      conf.getBoolean("spark.storage.decommission.enabled", false) &&
      conf.getBoolean("spark.storage.decommission.shuffleBlocks.enabled", true)
    require(!migrateShuffleBlocks, "RDMA shuffle does not support migration of shuffle blocks")
  }
}
