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

class GlutenPropagateJoinBloomSuite extends QueryTest with SharedSparkSession {
  test("reuse a scalar Bloom filter below an earlier inner join") {
    val cls =
      scala.util.Try(Class.forName("org.apache.gluten.extension.columnar.PropagateJoinBloomFilter"))
    assume(cls.isSuccess, "requires Velox")
    val strategy = cls.get.getConstructor(classOf[SparkSession])
      .newInstance(spark).asInstanceOf[SparkStrategy]
    def relation(sql: String): LogicalPlan = spark.sql(sql).queryExecution.optimizedPlan
    val a = relation("SELECT * FROM VALUES (1L, 7L), (1L, 7L), (2L, 8L), (3L, NULL)" +
      " AS x(id, nation)")
    val b = relation("SELECT * FROM VALUES (1L, 31L), (2L, 33L), (3L, 32L) AS y(fk, sk)")
    val c = relation("SELECT * FROM VALUES (31L, 7L), (31L, 7L), (33L, 8L), (32L, NULL)" +
      " AS z(supplier, country)")
    val d = relation("SELECT * FROM VALUES (7L), (NULL) AS n(nation)")
    val first = Join(a, b, Inner, Some(EqualTo(a.output.head, b.output.head)), JoinHint.NONE)
    val function = new BloomFilterAggregate(new XxHash64(Seq(d.output.head)))
    val sub = ScalarSubquery(
      Aggregate(Nil, Seq(Alias(function.toAggregateExpression(), "bf")()), d),
      Nil)
    val pred = BloomFilterMightContain(sub, new XxHash64(Seq(c.output.last)))
    val equality = EqualTo(a.output.last, c.output.last)
    val joined = Join(
      first,
      Filter(pred, c),
      Inner,
      Some(And(EqualTo(b.output.last, c.output.head), equality)),
      JoinHint.NONE)
    def rows(p: LogicalPlan): Seq[String] =
      spark.sessionState.executePlan(p).executedPlan.executeCollect().toSeq.map(_.toString).sorted
    val name = "spark.gluten.sql.columnar.backend.velox.propagateJoinBloomFilter.enabled"
    withSQLConf(name -> "true", "spark.sql.execution.reuseSubquery" -> "true") {
      val Seq(PlanLater(rewritten: Join)) = strategy(joined)
      val filters = rewritten.left.collect { case f: Filter => f }
      assert(filters.size == 1 && filters.head.child.fastEquals(a))
      val copied = filters.head.condition.asInstanceOf[BloomFilterMightContain]
      assert(copied.bloomFilterExpression eq sub)
      assert(copied.valueExpression.references == AttributeSet(Seq(a.output.last)))
      assert(rewritten.right.fastEquals(joined.right))
      assert(strategy(rewritten).isEmpty)
      val expected = rows(joined)
      assert(expected.size == 4 && rows(rewritten) == expected)
      assert(strategy(joined.copy(condition = Some(equality))).isEmpty)
      assert(strategy(joined.copy(joinType = LeftOuter)).isEmpty)
      assert(strategy(joined.copy(left = first.copy(joinType = LeftOuter))).isEmpty)
    }
    withSQLConf(name -> "false")(assert(strategy(joined).isEmpty))
    withSQLConf("spark.sql.execution.reuseSubquery" -> "false") {
      assert(strategy(joined).isEmpty)
    }
  }
}
