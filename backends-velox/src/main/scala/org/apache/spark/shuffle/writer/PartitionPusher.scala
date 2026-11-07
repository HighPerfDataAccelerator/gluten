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
package org.apache.spark.shuffle.writer

/**
 * Callback target of the native push-mode (RSS) partition writer. The native side looks this class
 * up by name and calls `pushPartitionData(I[BI)I` once per evicted payload.
 *
 * This build reuses the class for the pipelined (RTM streaming) shuffle; the Uniffle source set,
 * which defines a Uniffle-specific variant, is not compiled into the same jar.
 */
class PartitionPusher(sink: (Int, Array[Byte], Int) => Int) {
  def pushPartitionData(partitionId: Int, buffer: Array[Byte], length: Int): Int =
    sink(partitionId, buffer, length)
}
