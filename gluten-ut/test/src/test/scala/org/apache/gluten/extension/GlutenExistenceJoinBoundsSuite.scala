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

import org.apache.gluten.extension.columnar.ReduceExistenceJoins

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, ScalarSubquery, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join}
import org.apache.spark.sql.test.SharedSparkSession

class GlutenExistenceJoinBoundsSuite extends QueryTest with SharedSparkSession {
  test("existence bounds preserve nulls, empty subsets and duplicate probe rows") {
    withTempPath {
      dir =>
        withTempView("bounds_items", "bounds_probes") {
          spark.sql("""SELECT * FROM VALUES
          (1,1,true),(1,2,false),(1,null,true),(2,3,false),(2,4,null),
          (3,9,true),(3,10,true),(4,null,true),(5,7,true),(5,7,true),(null,1,true)
          AS t(k,s,late)""").write.parquet(dir.getCanonicalPath)
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("bounds_items")
          spark.sql("""SELECT * FROM VALUES (1,1),(1,1),(1,2),(1,null),(2,9),(2,3),
          (3,9),(4,1),(5,7),(null,2),(6,6) AS t(k,s)""").createOrReplaceTempView("bounds_probes")
          val sqlText = """SELECT p.k,p.s FROM bounds_probes p
          LEFT SEMI JOIN bounds_items a ON p.k=a.k AND p.s<>a.s
          LEFT ANTI JOIN (SELECT * FROM bounds_items WHERE late) b
          ON b.k=p.k AND b.s<>p.s"""
          val previous = spark.experimental.extraOptimizations
          try {
            withSQLConf("spark.gluten.enabled" -> "true") {
              def verify(query: String, eligible: Boolean): Seq[Row] = {
                spark.experimental.extraOptimizations = previous
                val rows = spark.sql(query).collect().toSeq
                spark.experimental.extraOptimizations = previous :+ ReduceExistenceJoins
                val df = spark.sql(query)
                checkAnswer(df, rows)
                assert(
                  df.queryExecution.optimizedPlan.toString.contains("_matching_low") == eligible)
                rows
              }
              assert(verify(sqlText, true).map(_.toString).sorted ==
                Seq(Row(1, 1), Row(1, 1), Row(2, 9), Row(2, 3)).map(_.toString).sorted)
              verify(sqlText.replace("p.s<>a.s", "p.s=a.s"), false)
              verify(
                sqlText.replace("bounds_items a", "(SELECT * FROM bounds_items WHERE s > 1) a"),
                false)
            }
          } finally {
            spark.experimental.extraOptimizations = previous
          }
        }
    }
  }
  test("count matching self-existence rows before joining dimensions") {
    withTempPath {
      dir =>
        withTempView("counted_events", "counted_names") {
          spark.sql("""SELECT * FROM VALUES (1,1,true),(1,1,true),(1,2,false),
          (2,2,true),(2,7,false),(2,null,true),(3,1,true),(3,2,true),(4,1,false),
          (5,1,true),(5,1,true),(6,null,true),(7,null,true),(7,4,false),
          (8,3,true),(8,2,false),(null,1,true),(null,2,false) AS t(k,s,late)""")
            .write.parquet(dir.getCanonicalPath)
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("counted_events")
          spark.sql("SELECT * FROM VALUES (1,'a'),(1,'a'),(2,'a'),(3,'b') AS t(s,name)")
            .createOrReplaceTempView("counted_names")
          val text = """SELECT name, count(*) AS total FROM counted_events p
          JOIN counted_names d ON d.s=p.s WHERE p.late
          AND EXISTS (SELECT 1 FROM counted_events a WHERE p.k=a.k AND p.s<>a.s)
          AND NOT EXISTS (SELECT 1 FROM counted_events b WHERE p.k=b.k AND p.s<>b.s AND b.late)
          GROUP BY name"""
          val old = spark.experimental.extraStrategies
          try {
            def compare(q: String, expected: Boolean): Seq[Row] = {
              spark.experimental.extraStrategies = old
              val rows = spark.sql(q).collect().toSeq
              spark.experimental.extraStrategies = old :+ ReduceExistenceJoins.FoldExistenceCounts
              val df = spark.sql(q)
              checkAnswer(df, rows)
              assert(
                df.queryExecution.executedPlan.toString.contains("_qualifying_count") == expected)
              rows
            }
            assert(compare(text, true).toSet == Set(Row("a", 5L), Row("b", 1L)))
            compare(text.replace("count(*)", "count(DISTINCT p.s)"), false)
            compare(text.replace("WHERE p.late", "WHERE NOT p.late"), false)
            // The other supplier may fail the probe filter and still invalidate the order.
            for (supplier <- Seq(1, 2, 99)) {
              spark.experimental.extraStrategies = old
              val expected =
                spark.sql(text.replace("WHERE p.late", s"WHERE p.late AND p.s = $supplier"))
                  .collect().toSeq
              val source = spark.sql(s"SELECT $supplier AS s").queryExecution.optimizedPlan
              val bits = new BloomFilterAggregate(new XxHash64(Seq(source.output.head)))
              val lookup = ScalarSubquery(
                Aggregate(Nil, Seq(Alias(bits.toAggregateExpression(), "keys")()), source),
                Nil)
              val input = spark.sql(text).queryExecution.optimizedPlan.transformUp {
                case j: Join if j.joinType == LeftSemi =>
                  val value = j.left.output.find(_.name == "s").get
                  j.copy(left =
                    Filter(BloomFilterMightContain(lookup, new XxHash64(Seq(value))), j.left))
              }
              spark.experimental.extraStrategies = old :+ ReduceExistenceJoins.FoldExistenceCounts
              val plan = spark.sessionState.executePlan(input).executedPlan
              assert(plan.executeCollectPublic().toSeq == expected)
              assert(plan.toString.contains("_candidate_group_key"))
            }
          } finally {
            spark.experimental.extraStrategies = old
          }
        }
    }
  }

}
