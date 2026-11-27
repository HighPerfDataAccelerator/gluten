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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.test.SharedSparkSession

class GlutenShareGroupedThresholdSuite extends QueryTest with SharedSparkSession {
  test("a filtered rollup and its scalar threshold share the same grouped join") {
    val prefix = "org.apache.gluten.extension.columnar."
    val module = scala.util.Try(Class.forName(prefix + "ShareGroupedThreshold$"))
    assume(module.isSuccess, "requires Velox")
    val rule = module.get.getField("MODULE$").get(null).asInstanceOf[Rule[LogicalPlan]]
    val strategy = Class.forName(prefix + "PlanSharedGroupingInput")
      .getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[SparkStrategy]
    val config = "spark.gluten.sql.columnar.backend.velox.shareGroupedThreshold.enabled"
    val input = """WITH totals AS (
      SELECT category, color, bucket, SUM(amount) AS total
      FROM VALUES (1, 10), (1, 10), (2, 5), (2, NULL), (3, 4) AS f(k, amount)
      JOIN VALUES (1, 'red'), (1, 'red'), (2, 'blue'), (3, NULL) AS d(k, color)
      ON f.k = d.k
      JOIN VALUES (1, 'a', 1), (2, 'b', 1), (3, NULL, 2) AS t(k, category, bucket)
      ON f.k = t.k
      GROUP BY category, color, bucket) """
    val sql = input + """SELECT category, SUM(total) AS paid FROM totals WHERE color = 'red'
      GROUP BY category HAVING SUM(total) > (SELECT AVG(total) FROM totals)"""
    def analyzed(q: String): LogicalPlan = spark.sql(q).queryExecution.analyzed
    def markers(plan: LogicalPlan): Int = plan.collectWithSubqueries {
      case n if n.nodeName == "SharedGroupingInput" => n
    }.size
    val previous = spark.experimental.extraStrategies
    spark.experimental.extraStrategies = strategy +: previous
    try {
      withSQLConf(
        config -> "true",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.exchange.reuse" -> "true",
        "spark.sql.shuffle.partitions" -> "2") {
        Seq(
          sql,
          sql.replace("color = 'red'", "color IS NULL"),
          sql.replace("color = 'red'", "color = 'absent'"),
          sql.replace("ON f.k = t.k", "ON f.k = t.k AND t.k < 0")
        ).foreach {
          q =>
            val original = analyzed(q)
            val changed = rule(original)
            assert(markers(changed) == 2)
            assert(rule(changed).fastEquals(changed))
            def rows(p: LogicalPlan): Seq[String] =
              spark.sessionState.executePlan(p).executedPlan.executeCollect().toSeq.map(_.toString)
            assert(rows(original) == rows(changed))
        }
        val unsupported = Seq(
          sql.replace("AVG(total)", "MAX(total)"),
          sql.replace("AVG(total) FROM totals", "AVG(total) FROM totals WHERE bucket > 1"),
          sql.replace("SUM(amount)", "SUM(CAST(amount AS DOUBLE))"),
          sql.replace("SUM(amount) AS total", "SUM(amount) AS total, MAX(amount) AS unused"),
          sql + " UNION ALL SELECT category, total FROM totals",
          sql.replace(
            "AVG(total) FROM totals",
            "AVG(s.total) FROM totals s WHERE s.category = totals.category")
        )
        unsupported.foreach(q => assert(markers(rule(analyzed(q))) == 0))
      }
      Seq(config, "spark.sql.adaptive.enabled", "spark.sql.exchange.reuse").foreach {
        key => withSQLConf(key -> "false")(assert(markers(rule(analyzed(sql))) == 0))
      }
    } finally {
      spark.experimental.extraStrategies = previous
    }
  }
}
