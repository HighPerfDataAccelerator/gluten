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

import org.apache.gluten.extension.columnar.ProjectExpandMeasures

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.test.SharedSparkSession

class GlutenExpandMeasuresSuite extends QueryTest with SharedSparkSession {
  test("rollup shares an invariant decimal sum input and respects aggregate filters") {
    val previous = spark.experimental.extraOptimizations
    try {
      withSQLConf("spark.sql.ansi.enabled" -> "true", "spark.gluten.enabled" -> "true") {
        val from = """ FROM VALUES (0, 1.25, 4), (0, 3.00, 0), (1, NULL, 3),
          (1, 4.50, -2), (NULL, 9.00, NULL) AS t(k, price, q) GROUP BY ROLLUP(k)"""
        def check(sums: String, expectedMeasures: Int): Unit = {
          val query = "SELECT k, grouping_id(), " + sums + from
          spark.experimental.extraOptimizations = previous
          val expected = spark.sql(query).collect().toSeq
          spark.experimental.extraOptimizations = previous :+ ProjectExpandMeasures
          val df = spark.sql(query)
          checkAnswer(df, expected)
          val expansions = df.queryExecution.optimizedPlan.collect { case e: Expand => e }
          assert(expansions.size == 1)
          assert(expansions.head.output.count(_.name == "_expand_measure") == expectedMeasures)
        }
        check("sum(coalesce(price * q, 0.00)), sum(coalesce(price * q, 0.00))", 1)
        check("sum(10 div q) filter (where q <> 0)", 0)
      }
    } finally {
      spark.experimental.extraOptimizations = previous
    }
  }
}
