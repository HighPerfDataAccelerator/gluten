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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class GlutenScalarHavingKeysSuite extends QueryTest with SharedSparkSession {
  test("reuse exact joined HAVING keys while preserving scalar thresholds and duplicates") {
    assume(
      scala.util.Try(Class.forName(
        "org.apache.gluten.extension.columnar.ReuseFilteredFinalAggregate")).isSuccess,
      "requires Velox")
    val rule = Class.forName("org.apache.gluten.extension.columnar.ReuseFilteredFinalAggregate")
      .getConstructor(classOf[org.apache.spark.sql.SparkSession]).newInstance(spark)
      .asInstanceOf[Rule[SparkPlan]]
    val key = "spark.gluten.sql.columnar.backend.velox.reuseScalarHavingKeys.enabled"
    val query = """WITH agg AS (
      SELECT c.id, sum(cast(s.id % 13 AS decimal(18,2))) AS v
      FROM range(1000) s JOIN range(11) c ON s.id % 11 = c.id GROUP BY c.id
      ), eligible AS (SELECT id FROM agg WHERE v > 0.95 * (SELECT max(v) FROM agg))
      SELECT id FROM eligible UNION ALL SELECT id FROM eligible"""
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "4",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.codegen.wholeStage" -> "false",
      "spark.gluten.sql.columnar.backend.velox.reuseFilteredFinalAggregate.enabled" -> "false"
    ) {
      def plan(sql: String): SparkPlan = spark.sql(sql).queryExecution.executedPlan
      def exchanges(p: SparkPlan): Int = p.collect { case _: ShuffleExchangeExec => 1 }.size
      val input = plan(query)
      withSQLConf(key -> "false")(assert(rule(input) eq input))
      withSQLConf(key -> "true") {
        val shared = rule(input)
        assert(exchanges(shared) == exchanges(input) + 2, input.treeString)
        assert(shared.output == input.output)
        assert(rule(shared).fastEquals(shared))
        assert(shared.executeCollectPublic().toSeq.sortBy(_.getLong(0)) ==
          input.executeCollectPublic().toSeq.sortBy(_.getLong(0)))
        val sampled = plan(query.replace("SELECT max(v) FROM agg", "SELECT first(v) FROM agg"))
        assert(rule(sampled) eq sampled)
        withSQLConf("spark.sql.execution.reuseSubquery" -> "false") {
          assert(rule(input) eq input)
        }
      }
    }
  }
}
