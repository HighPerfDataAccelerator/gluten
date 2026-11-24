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
}
