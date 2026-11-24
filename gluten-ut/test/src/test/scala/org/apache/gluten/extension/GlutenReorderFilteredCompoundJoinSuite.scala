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
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{PlanLater, SparkStrategy}
import org.apache.spark.sql.test.SharedSparkSession

class GlutenReorderFilteredCompoundJoinSuite extends QueryTest with SharedSparkSession {
  test("reassociate an unfiltered dimension with a bridge under a common selective filter") {
    val cls = scala.util.Try(
      Class.forName("org.apache.gluten.extension.columnar.ReorderFilteredCompoundJoin"))
    assume(cls.isSuccess, "requires Velox")
    val strategy =
      cls.get.getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[SparkStrategy]
    def values(rows: Seq[String], names: String): LogicalPlan =
      spark.sql(
        s"SELECT * FROM VALUES ${rows.mkString(",")} AS t($names)").queryExecution.optimizedPlan
    val fact = values(
      (0 until 800).map(i => s"(${i % 4}L,${i % 11}L,$i)") :+ "(NULL, 1L, 0)",
      "fk, part, value")
    val bridge =
      values((0 until 60).map(i => s"(${i % 4}L,${i % 11}L,$i)") :+ "(1L, NULL, 0)", "bk, bp, cost")
    val dimension = values(Seq("(1L,1)", "(1L,2)", "(2L,3)", "(NULL,4)"), "dk, payload")
    val selected = values(Seq("(1L)", "(2L)", "(NULL)"), "pk")
    def bloom(input: LogicalPlan, attr: Attribute): Expression = {
      val fun = new BloomFilterAggregate(new XxHash64(Seq(input.output.head)))
      val scalar =
        ScalarSubquery(Aggregate(Nil, Seq(Alias(fun.toAggregateExpression(), "bf")()), input), Nil)
      BloomFilterMightContain(scalar, new XxHash64(Seq(attr)))
    }
    val left = Join(
      Filter(bloom(selected, fact.output(1)), fact),
      selected,
      Inner,
      Some(EqualTo(fact.output(1), selected.output.head)),
      JoinHint.NONE)
    val first = Join(
      left,
      dimension,
      Inner,
      Some(EqualTo(fact.output.head, dimension.output.head)),
      JoinHint.NONE)
    val right = Filter(bloom(selected, bridge.output(1)), bridge)
    val single = EqualTo(bridge.output.head, fact.output.head)
    val both = And(single, EqualTo(bridge.output(1), fact.output(1)))
    val original = Join(
      Project(fact.output ++ Seq(dimension.output(1)), first),
      right,
      Inner,
      Some(both),
      JoinHint.NONE)
    def rows(p: LogicalPlan): Seq[String] =
      spark.sessionState.executePlan(p).executedPlan.executeCollect().toSeq.map(_.toString).sorted
    val enabled = "spark.gluten.sql.columnar.backend.velox.joinReorder.filteredCompound.enabled"
    withSQLConf(
      enabled -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.shuffle.partitions" -> "2") {
      val Seq(PlanLater(changed: Project)) = strategy(original)
      assert(changed.output == original.output)
      val top = changed.child.asInstanceOf[Join]
      val attached = top.right.asInstanceOf[Join]
      assert(top.condition == Some(both))
      assert(attached.left.fastEquals(right) && attached.right.fastEquals(dimension))
      assert(attached.condition.contains(EqualTo(bridge.output.head, dimension.output.head)))
      val expected = rows(original)
      assert(expected.nonEmpty && rows(changed) == expected)
      assert(strategy(original.copy(condition = Some(single))).isEmpty)
      assert(strategy(original.copy(joinType = LeftOuter)).isEmpty)
      assert(strategy(original.copy(right = bridge)).isEmpty)
      assert(strategy(original.copy(right =
        Filter(bloom(dimension, bridge.output(1)), bridge))).isEmpty)
      val filtered = first.copy(right = Filter(EqualTo(dimension.output(1), Literal(1)), dimension))
      assert(strategy(original.copy(left = filtered)).isEmpty)
      withSQLConf(enabled -> "false")(assert(strategy(original).isEmpty))
      withSQLConf(
        "spark.sql.autoBroadcastJoinThreshold" -> "8m")(assert(strategy(original).isEmpty))
    }
  }
}
