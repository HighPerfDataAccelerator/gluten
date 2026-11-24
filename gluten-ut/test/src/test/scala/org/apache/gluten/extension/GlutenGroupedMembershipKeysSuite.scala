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
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.test.SharedSparkSession

class GlutenGroupedMembershipKeysSuite extends QueryTest with SharedSparkSession {
  test("share only repeated one-key IN consumers of a deterministic grouped join") {
    val prefix = "org.apache.gluten.extension.columnar."
    val cls = scala.util.Try(Class.forName(prefix + "ShareGroupedMembershipKeys$"))
    assume(cls.isSuccess, "requires Velox")
    val rule = cls.get.getField("MODULE$").get(null).asInstanceOf[Rule[LogicalPlan]]
    val strategy = Class.forName(prefix + "PlanMembershipKeys")
      .getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[SparkStrategy]
    val source = """FROM VALUES (1, 3, 0), (1, 3, 1), (2, NULL, 2), (3, 5, 3) AS f(k, item, bucket)
      JOIN VALUES (1), (1), (2), (2), (3) AS d(id) ON k=id"""
    val defn = s"""WITH fset AS (SELECT item, bucket, count(*) n $source
      GROUP BY item, bucket HAVING count(*) >= 2) """
    val outer = """SELECT key, key IN (SELECT item FROM fset) AS present,
      key NOT IN (SELECT item FROM fset) AS absent FROM VALUES (3), (5), (NULL) AS t(key)"""
    val sql = defn + outer
    val general = "spark.gluten.sql.columnar.backend.velox.reuseFilteredFinalAggregate.enabled"
    val scalar = "spark.gluten.sql.columnar.backend.velox.reuseScalarHavingKeys.enabled"
    def analyzed(q: String): LogicalPlan = spark.sql(q).queryExecution.analyzed
    def markers(p: LogicalPlan): Seq[LogicalPlan] =
      p.collectWithSubqueries { case n if n.nodeName == "SharedMembershipKeys" => n }
    def rows(p: LogicalPlan): Seq[String] =
      spark.sessionState.executePlan(p).executedPlan.executeCollect().toSeq.map(_.toString).sorted
    val old = spark.experimental.extraStrategies
    spark.experimental.extraStrategies = strategy +: old
    try {
      withSQLConf(
        general -> "true",
        scalar -> "true",
        "spark.sql.shuffle.partitions" -> "2",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.exchange.reuse" -> "true"
      ) {
        val before = analyzed(sql)
        val after = rule(before)
        assert(
          markers(after).size == 2 && markers(after).forall(_.children.head.isInstanceOf[Distinct]))
        assert(rule(after).fastEquals(after))
        assert(rows(before) == rows(after))
        val answers = spark.sql(sql).collect()
        assert(answers.length == 3 && answers.exists(
          r =>
            r.get(0) == 3 && r.getBoolean(1) && !r.getBoolean(2)))
        val threshold = s"""WITH lim AS (SELECT MAX(x) AS maximum FROM VALUES (1), (2) AS t(x)),
          fset AS (SELECT item, SUM(CAST(bucket AS DECIMAL(10,0))) AS total $source
          GROUP BY item HAVING SUM(CAST(bucket AS DECIMAL(10,0))) > (SELECT maximum FROM lim)) """
        val original = analyzed(threshold + outer)
        val changed = rule(original)
        assert(markers(changed).size == 2 && !markers(
          changed).exists(_.children.head.isInstanceOf[Distinct]))
        assert(rows(changed) == rows(original))
        withSQLConf(general -> "false")(assert(markers(rule(before)).isEmpty))
        withSQLConf(
          "spark.sql.execution.reuseSubquery" -> "false")(assert(markers(rule(original)).isEmpty))
        withSQLConf(scalar -> "false")(assert(markers(rule(original)).isEmpty))
        withSQLConf(
          "spark.sql.autoBroadcastJoinThreshold" -> "8m")(assert(markers(rule(before)).isEmpty))
        withSQLConf("spark.sql.exchange.reuse" -> "false")(assert(markers(rule(before)).isEmpty))
        assert(markers(rule(analyzed(sql.replace(
          "key NOT IN (SELECT item FROM fset)",
          "FALSE")))).isEmpty)
        assert(
          markers(rule(analyzed(sql + " UNION ALL SELECT item, TRUE, TRUE FROM fset"))).isEmpty)
        assert(markers(rule(analyzed(threshold.replace("MAX(x)", "FIRST(x)") + outer))).isEmpty)
      }
    } finally {
      spark.experimental.extraStrategies = old
    }
  }
}
