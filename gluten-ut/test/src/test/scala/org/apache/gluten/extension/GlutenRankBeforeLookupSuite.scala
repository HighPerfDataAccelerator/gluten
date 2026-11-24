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
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, ExprId}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.test.SharedSparkSession

class GlutenRankBeforeLookupSuite extends QueryTest with SharedSparkSession {
  test("move bounded rank across generated lookups only when its inputs are on the left") {
    val cls = scala.util.Try(Class.forName("org.apache.gluten.extension.columnar.RankBeforeLookup$"))
    assume(cls.isSuccess, "requires the Velox grouping-key rule")
    val module = cls.get.getField("MODULE$").get(null)
    val tag = cls.get.getMethod("lookupKey").invoke(module).asInstanceOf[TreeNodeTag[ExprId]]
    val applyRule = cls.get.getMethod("apply", classOf[LogicalPlan])
    def rewrite(plan: LogicalPlan): LogicalPlan =
      applyRule.invoke(module, plan).asInstanceOf[LogicalPlan]
    def rows(plan: LogicalPlan): Seq[String] =
      spark.sessionState.executePlan(plan).executedPlan.executeCollect().toSeq.map(_.toString).sorted

    def check(function: String, ordering: String, marked: Boolean, moves: Boolean): Unit = {
      val text =
        s"""SELECT k, label, score, rk FROM (
           | SELECT k, label, score, $function() OVER (PARTITION BY grp ORDER BY $ordering) rk
           | FROM (SELECT k, grp, score, label FROM
           |   (VALUES (1, 5, 8), (1, 5, 8), (2, 5, 7), (NULL, 5, 10),
           |           (4, NULL, 7), (5, NULL, 7), (6, 5, 4)) f(k,grp,score)
           | LEFT JOIN (VALUES (1,'x'),(2,'y'),(4,NULL),(5,'x')) d(id,label)
           | ON f.k = d.id) x) y WHERE rk <= 2""".stripMargin
      val plan = spark.sql(text).queryExecution.optimizedPlan
      val Seq(lookup) = plan.collect { case j: Join if j.joinType == LeftOuter => j }
      if (marked) {
        val Some(EqualTo(_, id: Attribute)) = lookup.condition
        lookup.setTagValue(tag, id.exprId)
      }
      val changed = rewrite(plan)
      assert(changed.output == plan.output)
      val moved = changed.exists {
        case j: Join => j.left.exists(_.isInstanceOf[Window])
        case _ => false
      }
      assert(moved == moves, changed.treeString)
      assert(rows(changed) == rows(plan))
    }

    withSQLConf("spark.sql.shuffle.partitions" -> "2") {
      check("rank", "score DESC", marked = true, moves = true)
      check("dense_rank", "score DESC", marked = true, moves = true)
      check("rank", "score DESC", marked = false, moves = false)
      check("rank", "label DESC", marked = true, moves = false)
      check("row_number", "score DESC, k", marked = true, moves = false)
    }
  }
}
