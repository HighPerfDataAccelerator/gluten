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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.test.SharedSparkSession

class GlutenIntersectKeysSuite extends QueryTest with SharedSparkSession {
  test("aggregate shared dimension keys in a chain of distinct intersections") {
    val module =
      scala.util.Try(Class.forName("org.apache.gluten.extension.columnar.AggregateIntersectKeys$"))
    assume(module.isSuccess, "requires Velox")
    val rule = module.get.getField("MODULE$").get(null).asInstanceOf[Rule[LogicalPlan]]
    val setting = "spark.gluten.sql.columnar.backend.velox.aggregateIntersectKeys.enabled"
    withTempView("intersect_dim") {
      spark.sql("SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 5), (3, 5), " +
        "(4, -1), (5, 7) AS d(id, k)").createOrReplaceTempView("intersect_dim")
      def branch(ids: Int*): String = {
        val tuples = ids.map(i => s"($i)").mkString(", ")
        s"SELECT d.k AS k FROM intersect_dim d JOIN VALUES $tuples AS f(id) ON d.id = f.id"
      }
      val a = branch(1, 1, 2, 4)
      val b = branch(1, 3, 3, 4, 5)
      val c = branch(1, 2, 5)
      def plan(sql: String): LogicalPlan = spark.sql(sql).queryExecution.analyzed
      def values(p: LogicalPlan): Seq[Option[Int]] = {
        spark.sessionState.executePlan(p).executedPlan.executeCollect().toSeq.map {
          row => if (row.isNullAt(0)) None else Some(row.getInt(0))
        }.sortBy(_.getOrElse(Int.MinValue))
      }

      val original = plan(s"$a INTERSECT $b INTERSECT $c")
      withSQLConf(setting -> "true") {
        val rewritten = rule(original)
        assert(rewritten.treeString.contains("_intersect_mask"))
        assert(rewritten.output.map(_.exprId) == original.output.map(_.exprId))
        assert(rewritten.schema == original.schema)
        assert(rule(rewritten).fastEquals(rewritten))
        assert(values(rewritten) == Seq(None, Some(5)))
        assert(values(rewritten) == values(original))
        val empty = plan(s"$a INTERSECT $b INTERSECT ${branch(9)}")
        assert(values(rule(empty)).isEmpty)

        Seq(
          s"$a INTERSECT $b",
          s"$a INTERSECT ALL $b INTERSECT $c",
          s"$a INTERSECT $b INTERSECT ${c.replace("d.k AS k", "d.id AS k")}"
        ).foreach {
          sql =>
            val before = plan(sql)
            assert(rule(before).fastEquals(before))
        }
      }
      withSQLConf(setting -> "false") {
        assert(rule(original).fastEquals(original))
      }
    }
  }
}
