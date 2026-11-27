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
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.storage.BlockManagerId

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

class RdmaShuffleTransportSuite extends AnyFunSuite {
  private def encrypted = new SparkConf(false)
    .set("spark.authenticate", "true")
    .set("spark.network.crypto.enabled", "true")

  test("accept authenticated encrypted executor-served shuffle") {
    RdmaShuffleTransport.validate(encrypted)
  }

  test("reject unsupported transports and security configurations") {
    Seq(
      "spark.authenticate" -> "false",
      "spark.network.crypto.enabled" -> "false",
      "spark.ssl.rpc.enabled" -> "true",
      "spark.ssl.enabled" -> "true",
      "spark.shuffle.service.enabled" -> "true",
      "spark.shuffle.push.enabled" -> "true"
    ).foreach {
      case (key, value) =>
        withClue(key) {
          intercept[IllegalArgumentException] {
            RdmaShuffleTransport.validate(encrypted.set(key, value))
          }
        }
    }
  }

  test("reject migration of shuffle blocks during decommissioning") {
    val conf = encrypted
      .set("spark.decommission.enabled", "true")
      .set("spark.storage.decommission.enabled", "true")
    intercept[IllegalArgumentException] {
      RdmaShuffleTransport.validate(conf)
    }
    RdmaShuffleTransport.validate(conf.set(
      "spark.storage.decommission.shuffleBlocks.enabled",
      "false"))
  }

  test(
    "reuse the peer relay, keep same-host fetches direct, and report closed transport failures") {
    val script = Files.createTempFile("rdma-test-relay", ".sh")
    val launches = Files.createTempFile("rdma-test-relay", ".log")
    val pids = Files.createTempFile("rdma-test-relay", ".pids")
    try {
      Files.write(
        script,
        s"""#!/bin/sh
           |echo "$$@" >> '$launches'
           |echo $$$$ >> '$pids'
           |echo 23456
           |exec cat >/dev/null
           |""".stripMargin.getBytes(UTF_8)
      )
      assert(script.toFile.setExecutable(true, true))
      val calls = ArrayBuffer.empty[(String, Int)]
      val failed = ArrayBuffer.empty[String]
      val delegate = new BlockStoreClient {
        override def fetchBlocks(
            host: String,
            port: Int,
            execId: String,
            ids: Array[String],
            listener: BlockFetchingListener,
            files: DownloadFileManager): Unit = {
          calls += ((host, port))
        }

        override def close(): Unit = fail("the underlying Spark client belongs to BlockManager")
      }
      val listener = new BlockFetchingListener {
        override def onBlockFetchSuccess(id: String, data: ManagedBuffer): Unit = fail(id)
        override def onBlockFetchFailure(id: String, cause: Throwable): Unit = failed += id
      }
      val conf = encrypted
        .set("spark.gluten.shuffle.rdma.executable", script.toString)
        .set("spark.gluten.shuffle.rdma.address.remote", "192.0.2.2")
      val transport = new RdmaShuffleTransport(conf, BlockManagerId("0", "local", 1024), delegate)
      val ids = Array("shuffle_1_2_3", "shuffle_1_2_4")
      try {
        transport.fetchBlocks("local", 1025, "1", ids, listener, null)
        for (_ <- 1 to 2) transport.fetchBlocks("remote", 1026, "2", ids, listener, null)
        assert(calls.toList == List(("local", 1025), ("127.0.0.1", 23456), ("127.0.0.1", 23456)))
        val log = Files.readAllLines(launches)
        assert(log.size() == 2)
        assert(log.get(0) == "ingress local 1024 local 1024")
        assert(log.get(1) == "egress 127.0.0.1 0 192.0.2.2 1026")

        val child = ProcessHandle.of(Files.readAllLines(pids).get(1).toLong).get()
        assert(child.destroyForcibly())
        child.onExit().get(5, TimeUnit.SECONDS)
        transport.fetchBlocks("remote", 1026, "2", ids, listener, null)
        assert(Files.readAllLines(launches).get(2) == "egress 127.0.0.1 23456 192.0.2.2 1026")
        assert(calls.last == (("127.0.0.1", 23456)))
      } finally {
        transport.close()
      }
      transport.fetchBlocks("remote", 1026, "2", ids, listener, null)
      assert(failed.toList == ids.toList && calls.size == 4)
    } finally {
      Files.deleteIfExists(script)
      Files.deleteIfExists(launches)
      Files.deleteIfExists(pids)
    }
  }
}
