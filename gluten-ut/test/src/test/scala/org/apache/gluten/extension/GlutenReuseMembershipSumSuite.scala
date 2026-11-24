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
import org.apache.spark.sql.catalyst.expressions.{CheckOverflowInSum, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Decimal, DecimalType}

class GlutenReuseMembershipSumSuite extends QueryTest with SharedSparkSession {
  test("reuse a checked subtotal while preserving duplicate and null keys") {
    val cls =
      scala.util.Try(Class.forName("org.apache.gluten.extension.columnar.ReuseMembershipSum"))
    assume(cls.isSuccess, "requires the Velox subtotal strategy")
    val rule =
      cls.get.getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[SparkStrategy]
    val setting = "spark.gluten.sql.columnar.backend.velox.reuseGroupedDecimalSum.enabled"
    val conf = spark.experimental.extraStrategies
    withTempDir {
      dir =>
        try {
          Seq(
            "dim" -> "SELECT * FROM VALUES (1L,'a'),(1L,'a'),(1L,'b'),(2L,'a'),(3L,'n'),(NULL,'z') d(k,label)",
            "fact" -> """SELECT k,cast(v AS DECIMAL(15,2)) v FROM VALUES
              (1L,900),(1L,-870),(1L,NULL),(2L,-5),(2L,20),(3L,NULL),(NULL,30) f(k,v)"""
          ).foreach {
            case (name, text) =>
              val path = new java.io.File(dir, name).toString
              spark.sql(text).write.parquet(path)
              spark.read.parquet(path).createOrReplaceTempView(name)
          }
          spark.experimental.extraStrategies = rule +: conf
          val query = """SELECT d.k,d.label,SUM(f.v) AS total FROM dim d, fact f
          WHERE d.k IN (SELECT k FROM fact GROUP BY k HAVING SUM(v)>10)
          AND d.k=f.k GROUP BY d.k,d.label"""
          def compare(text: String, expectedRewrite: Boolean): Seq[String] = {
            val (rows, schema) = withSQLConf(setting -> "false") {
              val df = spark.sql(text)
              (df.collect().map(_.toString).sorted.toList, df.schema)
            }
            withSQLConf(setting -> "true") {
              val df = spark.sql(text)
              assert(df.schema == schema)
              assert(df.collect().map(_.toString).sorted.toList == rows)
              assert(df.queryExecution.executedPlan.treeString.contains(
                "grouped_subtotal") == expectedRewrite)
            }
            rows
          }
          withSQLConf(
            "spark.sql.shuffle.partitions" -> "2",
            "spark.sql.autoBroadcastJoinThreshold" -> "-1") {
            assert(compare(
              query,
              expectedRewrite = true) == Seq("[1,a,60.00]", "[1,b,30.00]", "[2,a,15.00]"))
            assert(compare(query.replace(">10", ">9999"), expectedRewrite = true).isEmpty)
            compare(
              query.replace("AND d.k=f.k", "AND f.v > 0 AND d.k=f.k"),
              expectedRewrite = false)
            compare(query.replace("SUM(f.v)", "SUM(DISTINCT f.v)"), expectedRewrite = false)
            compare(
              query.replace(
                "SELECT d.k,d.label",
                "SELECT d.label").replace("GROUP BY d.k,d.label", "GROUP BY d.label"),
              expectedRewrite = false)
          }
        } finally {
          spark.experimental.extraStrategies = conf
          spark.catalog.dropTempView("dim")
          spark.catalog.dropTempView("fact")
        }
    }
  }

  test("use the decimal sum's final overflow check on a widened subtotal") {
    Seq(false, true).foreach {
      ansi =>
        withSQLConf("spark.sql.ansi.enabled" -> ansi.toString) {
          val sum = Sum(Literal.create(Decimal(1), DecimalType(15, 2)))
          val check = sum.evaluateExpression.collectFirst { case c: CheckOverflowInSum => c }.get
          def checked(value: String): Any =
            check.withNewChildren(Seq(Literal.create(
              Decimal(new java.math.BigDecimal(value)),
              DecimalType(35, 2)))).eval()
          val limit = "9" * 23 + ".99"
          assert(checked(limit).toString == limit)
          assert(checked("-" + limit).toString == "-" + limit)
          val overflow = "1" + "0" * 23 + ".00"
          if (ansi) intercept[ArithmeticException](checked(overflow))
          else assert(checked(overflow) == null)
        }
    }
  }
}
