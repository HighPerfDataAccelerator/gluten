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
package org.apache.gluten.extension

import org.apache.gluten.extension.columnar.ReuseSemiJoinInput

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class GlutenSemiJoinInputReuseSuite extends QueryTest with SharedSparkSession {
  test("an equality semi join can reuse a matching Parquet input shuffle") {
    withTempPath {
      path =>
        spark.sql("""
                    |SELECT cast(k as bigint) k, cast(v as decimal(12,2)) v, spare
                    |FROM VALUES (1, 4, 0), (1, 4, 1), (1, -1, 2), (2, -2, 3),
                    |            (3, null, 4), (null, 10, 5), (null, 12, 6) AS t(k, v, spare)
                    |""".stripMargin).write.parquet(path.getCanonicalPath)
        withTempView("semi_shuffle_facts") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("semi_shuffle_facts")
          val sqlText =
            """
              |SELECT f.k, f.v FROM semi_shuffle_facts f LEFT SEMI JOIN
              |  (SELECT k FROM (SELECT /*+ REPARTITION(4, k) */ k, v FROM semi_shuffle_facts) s
              |   GROUP BY k HAVING sum(v) > 0) g ON f.k = g.k
              |""".stripMargin
          val previous = spark.experimental.extraStrategies
          try {
            withSQLConf(
              "spark.gluten.enabled" -> "false",
              "spark.sql.shuffle.partitions" -> "4",
              "spark.sql.autoBroadcastJoinThreshold" -> "-1",
              "spark.sql.adaptive.enabled" -> "true",
              "spark.sql.adaptive.coalescePartitions.enabled" -> "false"
            ) {
              def result(query: String, reuse: Boolean): (Seq[String], Int) = {
                spark.experimental.extraStrategies =
                  if (reuse) ReuseSemiJoinInput(spark) +: previous else previous
                val df = spark.sql(query)
                val rows = df.collect().map(_.toString).sorted.toSeq
                val plan = df.queryExecution.executedPlan.toString.split("== Initial Plan ==")(0)
                (rows, plan.linesIterator.count(_.contains("FileScan parquet")))
              }
              val baseline = result(sqlText, reuse = false)
              val shared = result(sqlText, reuse = true)
              assert(shared._1 == baseline._1 && shared._1.size == 3)
              assert(baseline._2 == 2 && shared._2 == 1)
              val nullSafe = sqlText.replace("f.k = g.k", "f.k <=> g.k")
              val nullBaseline = result(nullSafe, reuse = false)
              assert(nullBaseline._1.size == 5)
              assert(result(nullSafe, reuse = true) == nullBaseline)
            }
          } finally {
            spark.experimental.extraStrategies = previous
          }
        }
    }
  }
}
