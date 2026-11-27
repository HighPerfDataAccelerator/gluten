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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{PlanLater, SparkStrategy}
import org.apache.spark.sql.test.SharedSparkSession

class GlutenFactorGroupedJoinCountsSuite extends QueryTest with SharedSparkSession {
  test("factor row counts while retaining both sides' group and join multiplicities") {
    val cls = scala.util.Try(
      Class.forName("org.apache.gluten.extension.columnar.FactorGroupedJoinCounts"))
    assume(cls.isSuccess, "requires Velox")
    val strategy =
      cls.get.getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[SparkStrategy]
    def values(text: Seq[String], names: String): LogicalPlan =
      spark.sql(s"SELECT * FROM VALUES ${text.mkString(",")} AS t($names)").queryExecution.optimizedPlan
    val l = values((0 until 160).map(i => s"(${i % 3},${i % 7})") ++ Seq("(NULL,2)", "(1,NULL)"), "fk,period")
    val r = values(Seq("(1,'a')","(1,'a')","(1,NULL)","(1,NULL)","(2,'b')","(NULL,'c')"), "pk,name")
    val joined = Join(l, r, Inner, Some(EqualTo(l.output.head, r.output.head)), JoinHint.NONE)
    val count = Alias(Count(Seq(Literal(1))).toAggregateExpression(), "n")()
    val enabled = "spark.gluten.sql.columnar.backend.velox.aggregation.factorJoinCounts.enabled"
    def rows(p: LogicalPlan) = spark.sessionState.executePlan(p).executedPlan.executeCollectPublic().toSeq.map(_.toString).sorted
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "128", enabled -> "true", "spark.sql.shuffle.partitions" -> "2") {
      for (key <- Seq(l.output.head, r.output.head)) {
        val keys = Seq(key, l.output(1), r.output(1))
        val agg = Aggregate(keys, keys :+ count, joined)
        val Seq(PlanLater(result: Project)) = strategy(agg)
        assert(result.output == agg.output)
        val expected = rows(agg)
        assert(expected.nonEmpty && rows(result) == expected)
        assert(strategy(agg.copy(child = joined.copy(joinType = LeftOuter))).isEmpty)
        assert(strategy(agg.copy(groupingExpressions = keys.tail)).isEmpty)
        val nullable = Alias(Count(Seq(r.output(1))).toAggregateExpression(), "n")()
        assert(strategy(agg.copy(aggregateExpressions = keys :+ nullable)).isEmpty)
        withSQLConf(enabled -> "false")(assert(strategy(agg).isEmpty))
      }
    }
  }
}
