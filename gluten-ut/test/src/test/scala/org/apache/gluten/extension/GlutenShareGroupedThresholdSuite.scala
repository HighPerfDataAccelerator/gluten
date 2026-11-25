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

  test("share joined input of a grouped decimal sum and its scalar threshold") {
    val prefix = "org.apache.gluten.extension.columnar."
    val module = scala.util.Try(Class.forName(prefix + "ShareGroupedThreshold$"))
    assume(module.isSuccess, "requires Velox")
    val rule = module.get.getField("MODULE$").get(null).asInstanceOf[Rule[LogicalPlan]]
    val strategy = Class.forName(prefix + "PlanSharedGroupingInput")
      .getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[SparkStrategy]
    val config = "spark.gluten.sql.columnar.backend.velox.shareGroupedThreshold.enabled"
    val joined = """FROM VALUES (1, 3.00, 2), (1, NULL, 2), (2, -1.50, 3),
      (3, 0.00, 1), (3, 4.50, 1), (4, NULL, 2) AS f(k, cost, qty)
      JOIN VALUES (1, 1), (1, 1), (2, 2), (3, 1), (4, 1) AS d(k, area) ON f.k = d.k
      JOIN VALUES (1), (2) AS n(area) ON d.area = n.area"""
    def query(inner: String = joined, amount: String = "cost * qty"): String =
      s"""SELECT f.k, SUM(cost * qty) AS total $joined GROUP BY f.k
         HAVING SUM(cost * qty) > (SELECT SUM($amount) * 0.1 $inner) ORDER BY f.k"""
    def analyzed(q: String): LogicalPlan = spark.sql(q).queryExecution.analyzed
    def markers(p: LogicalPlan): Int = p.collectWithSubqueries {
      case n if n.nodeName == "SharedGroupingInput" => n
    }.size
    def rows(p: LogicalPlan): Seq[String] =
      spark.sessionState.executePlan(p).executedPlan.executeCollect().toSeq.map(_.toString)
    val saved = spark.experimental.extraStrategies
    spark.experimental.extraStrategies = strategy +: saved
    try {
      withSQLConf(
        config -> "true",
        "spark.sql.shuffle.partitions" -> "2",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.exchange.reuse" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1"
      ) {
        Seq(
          query(),
          query().replace(joined, joined + " WHERE d.area < 0"),
          query().replace(joined, joined + " WHERE f.cost IS NULL")).foreach {
          sql =>
            val original = analyzed(sql)
            val changed = rule(original)
            assert(markers(changed) == 2)
            assert(rule(changed).fastEquals(changed))
            assert(original.schema == changed.schema)
            assert(rows(original) == rows(changed))
        }
        Seq(
          query(joined + " WHERE f.k > 1"),
          query(joined, "cost * (qty + 1)"),
          query().replace("SUM(cost", "SUM(DISTINCT cost"),
          query().replace("cost * qty", "CAST(cost AS DOUBLE) * qty"),
          query().replace("JOIN VALUES (1),", "LEFT JOIN VALUES (1),"),
          query(joined.replace(
            "AS f(",
            "AS fact(").replace("f.k =", "fact.k =") + " WHERE fact.k = f.k")
        ).foreach(q => assert(markers(rule(analyzed(q))) == 0))
        Seq(config, "spark.sql.adaptive.enabled", "spark.sql.exchange.reuse").foreach {
          key => withSQLConf(key -> "false")(assert(markers(rule(analyzed(query()))) == 0))
        }
        val facts = (0 until 200).map(i => s"(${i % 4}, 1.50, 2)").mkString(",")
        val from = s"""FROM VALUES $facts AS f(k,cost,qty),
          VALUES (1,1),(1,1),(2,2),(3,1),(4,1) AS d(k,area),
          VALUES (1),(2) AS n(area) WHERE f.k=d.k AND d.area=n.area"""
        val original = analyzed(query().replace(joined, from))
        def hashHints(p: LogicalPlan): Int = p.collectWithSubqueries {
          case j: org.apache.spark.sql.catalyst.plans.logical.Join
              if j.hint.rightHint.flatMap(_.strategy).contains(
                org.apache.spark.sql.catalyst.plans.logical.SHUFFLE_HASH) => j
        }.size
        assert(hashHints(rule(original)) == 0)
        withSQLConf(
          "spark.sql.autoBroadcastJoinThreshold" -> "16",
          "spark.sql.shuffle.partitions" -> "64") {
          val shared = rule(original)
          assert(markers(shared) == 2 && hashHints(shared) >= 2)
          assert(rows(original) == rows(shared))
        }
        withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "1g") {
          assert(markers(rule(analyzed(query()))) == 0)
        }
      }
    } finally {
      spark.experimental.extraStrategies = saved
    }
  }
}
