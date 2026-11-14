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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class GlutenQ5SecondHopBloomSuite extends QueryTest with SharedSparkSession {
  private val enabled = "spark.gluten.experimental.q5SecondHopBloom.enabled"
  private val cascade = "spark.gluten.sql.columnar.backend.velox.runtimeBloom.cascade."
  private val marker = "gluten_q5_second_hop_bloom_filter"
  private val firstMarker = "gluten_cascading_bloom_filter"
  private val scanThreshold =
    "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold"

  private def rule: Rule[LogicalPlan] = {
    if (
      scala.util.Try(Class.forName("org.apache.gluten.experimental.q5.Q5SecondHopBloom")).isFailure
    ) {
      cancel("The Q5 extension is not available for this backend")
    }
    Class.forName("org.apache.gluten.experimental.q5.Q5SecondHopBloom")
      .getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[Rule[LogicalPlan]]
  }

  private def bloom(key: Attribute, name: String): Expression = {
    val creationKey = AttributeReference("creation_key", LongType)()
    val values = LocalRelation(Seq(creationKey), Seq(InternalRow(1L), InternalRow(-1L)))
    val aggregate = new BloomFilterAggregate(new XxHash64(Seq(creationKey)))
    val creation = Aggregate(Nil, Seq(Alias(aggregate.toAggregateExpression(), name)()), values)
    BloomFilterMightContain(ScalarSubquery(creation, Nil), new XxHash64(Seq(key)))
  }

  private def input: Join = {
    val supplierKey = AttributeReference("s_suppkey", LongType)()
    val supplierNation = AttributeReference("s_nationkey", LongType)()
    val lineitemKey = AttributeReference("l_suppkey", LongType)()
    val customerNation = AttributeReference("c_nationkey", LongType)()
    val rows = Seq(InternalRow(1L, 1L), InternalRow(-1L, 1L))
    val supplier = LocalRelation(Seq(supplierKey, supplierNation), rows)
    val probe = LocalRelation(Seq(lineitemKey, customerNation), rows)
    Join(
      probe,
      Filter(bloom(supplierNation, firstMarker), supplier),
      Inner,
      Some(And(EqualTo(lineitemKey, supplierKey), EqualTo(customerNation, supplierNation))),
      JoinHint.NONE
    )
  }

  private def countFilters(plan: LogicalPlan): Int = plan.collect {
    case Filter(condition, _) => condition.collect {
        case BloomFilterMightContain(query: ScalarSubquery, _) if query.plan.exists {
              case aggregate: Aggregate => aggregate.aggregateExpressions.exists(_.name == marker)
              case _ => false
            } => 1
      }.sum
  }.sum

  private def active[A](body: => A): A = withSQLConf(
    enabled -> "true",
    "spark.gluten.enabled" -> "true",
    (cascade + "enabled") -> "true",
    (cascade + "maxFilters") -> "2",
    (cascade + "maxCreationKeyBytes") -> "268435456",
    scanThreshold -> "0"
  )(body)

  test("disabled rule and disabled dependencies leave the plan untouched") {
    val before = input
    active {
      Seq(enabled, "spark.gluten.enabled", cascade + "enabled").foreach {
        key => withSQLConf(key -> "false")(assert(rule.apply(before) eq before))
      }
    }
  }

  test("second-hop filter preserves both exact join keys and the supplier input") {
    val before = input
    active {
      val after = rule.apply(before)
      assert(countFilters(after) == 1)
      assert(after.output == before.output)
      val join = after.collectFirst { case j: Join => j }.get
      assert(join.joinType == before.joinType)
      assert(join.condition == before.condition)
      assert(join.right.fastEquals(before.right))
      assert(rule.apply(after).fastEquals(after))
    }
  }

  test("filter-count, projected-key size and application-side scan limits are enforced") {
    val before = input
    active {
      Seq(
        (cascade + "maxFilters") -> "1",
        (cascade + "maxCreationKeyBytes") -> "1",
        scanThreshold -> "1000000000").foreach {
        case (key, value) =>
          withSQLConf(key -> value)(assert(rule.apply(before) eq before))
      }
    }
  }

  test("outer joins and null-safe equality do not match the Q5 shape") {
    val before = input
    val And(first: EqualTo, second) = before.condition.get
    val nullSafe =
      before.copy(condition = Some(And(EqualNullSafe(first.left, first.right), second)))
    active {
      Seq(before.copy(joinType = LeftOuter), nullSafe).foreach {
        other => assert(rule.apply(other) eq other)
      }
    }
  }

  test("a first cascade wave is required and an existing supplier-key filter is retained") {
    val before = input
    val Filter(_, supplier) = before.right
    val noCascade = before.copy(right = supplier)
    val key = before.left.output.find(_.name == "l_suppkey").get
    val existing = before.copy(left = Filter(bloom(key, "existing_bloom"), before.left))
    active {
      assert(rule.apply(noCascade) eq noCascade)
      assert(rule.apply(existing) eq existing)
    }
  }

  test("an ambiguous plan with two matching joins is unchanged") {
    val before = Union(Seq(input, input))
    active(assert(rule.apply(before) eq before))
  }
}
