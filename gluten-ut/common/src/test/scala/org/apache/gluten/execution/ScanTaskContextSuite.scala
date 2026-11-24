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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

class ScanTaskContextSuite extends SparkFunSuite with SharedSparkSession {
  test("scan RDD serialization preserves the transform context when a task requires it") {
    for (stats <- Seq(false, true); gpu <- Seq(false, true)) {
      withSQLConf(GlutenConfig.GLUTEN_PASS_STAGE_INPUT_STATS_ENABLED.key -> stats.toString) {
        val plan = Array[Byte](4, 5, 6)
        val ctx = WholeStageTransformContext(null, enableCudf = gpu)
        val rdd = new GlutenWholeStageColumnarRDD(
          spark.sparkContext,
          Seq(GlutenPartition(0, plan)),
          new ColumnarInputRDDsWrapper(Seq.empty),
          SQLMetrics.createMetric(spark.sparkContext, "time"),
          _ => (),
          _ => (),
          gpu,
          ctx
        )

        var wroteContext = false
        val stream = new ObjectOutputStream(new ByteArrayOutputStream()) {
          enableReplaceObject(true)
          override def replaceObject(obj: AnyRef): AnyRef = {
            if (obj eq ctx) wroteContext = true
            obj
          }
        }
        try stream.writeObject(rdd)
        finally stream.close()

        val partition = rdd.partitions.head.asInstanceOf[FirstZippedPartitionsPartition]
        assert(partition.inputPartition.asInstanceOf[BaseGlutenPartition].plan.sameElements(plan))
        assert(wroteContext == (stats || gpu || BackendsApiManager.getBackendName != "velox"))
      }
    }
  }
}
