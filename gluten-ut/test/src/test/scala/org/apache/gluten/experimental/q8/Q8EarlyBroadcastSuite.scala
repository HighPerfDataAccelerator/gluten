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
package org.apache.gluten.experimental.q8

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Join}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

import java.sql.Date

import scala.io.Source
import scala.util.Try

/** Spark-only rule tests: native execution is covered by the separate Gluten experiment. */
class Q8EarlyBroadcastSuite extends QueryTest with SharedSparkSession {
  private val key = "spark.gluten.experimental.q8EarlyPartBroadcast.enabled"
  private val extensionName = "org.apache.gluten.experimental.q8.Q8EarlyBroadcastExtension"
  private lazy val extensionAvailable = Try(Class.forName(extensionName)).isSuccess
  private val selectedType = "MEDIUM BURNISHED COPPER"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", if (extensionAvailable) extensionName else "")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.shuffle.partitions", "2")
    .set("spark.sql.ansi.enabled", "false")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "false")

  private def decimal(value: String): java.math.BigDecimal = new java.math.BigDecimal(value)
  private def date(value: String): Date = Date.valueOf(value)

  private val tables = Seq(
    (
      "part",
      "p_partkey long, p_type string",
      Seq(
        Row(1L, selectedType),
        Row(1L, selectedType),
        Row(2L, selectedType),
        Row(-3L, selectedType),
        Row(4L, "OTHER"),
        Row(null, selectedType),
        Row(9L, selectedType))),
    ("supplier", "s_suppkey long, s_nationkey long", Seq(Row(10L, 10L), Row(11L, 11L))),
    (
      "nation",
      "n_nationkey long, n_name string, n_regionkey long",
      Seq(
        Row(10L, "MOROCCO", 1L),
        Row(11L, "ALGERIA", 1L),
        Row(20L, "USA", 2L))),
    ("region", "r_regionkey long, r_name string", Seq(Row(1L, "AFRICA"), Row(2L, "AMERICA"))),
    ("customer", "c_custkey long, c_nationkey long", Seq(Row(100L, 10L), Row(200L, 20L))),
    (
      "orders",
      "o_orderkey long, o_custkey long, o_orderdate date",
      Seq(
        Row(1000L, 100L, date("1995-01-01")),
        Row(1001L, 100L, date("1996-12-31")),
        Row(1002L, 200L, date("1995-06-01")),
        Row(1003L, 100L, date("1994-12-31")),
        Row(1004L, 100L, date("1997-01-01")),
        Row(1005L, 100L, date("1995-06-01")),
        Row(1006L, 100L, date("1996-06-01"))
      )),
    (
      "lineitem",
      "l_orderkey long, l_partkey long, l_suppkey long, " +
        "l_extendedprice decimal(15,2), l_discount decimal(15,2)",
      Seq(
        Row(1000L, 1L, 10L, decimal("100"), decimal(".1")),
        Row(1000L, 2L, 11L, decimal("100"), decimal(".1")),
        Row(1000L, -3L, 10L, decimal("50"), decimal("0")),
        Row(1000L, null, 10L, decimal("100"), decimal("0")),
        Row(1000L, 9L, 10L, null, decimal("0")),
        Row(1000L, 2L, 11L, decimal("200"), null),
        Row(1000L, 4L, 10L, decimal("100"), decimal("0")),
        Row(1001L, 2L, 11L, decimal("300"), decimal("0")),
        Row(1006L, -3L, 10L, decimal("100"), decimal("0")),
        Row(1005L, 1L, null, decimal("100"), decimal("0")),
        Row(null, 1L, 10L, decimal("100"), decimal("0")),
        Row(1002L, 1L, 10L, decimal("100"), decimal("0")),
        Row(1003L, 1L, 10L, decimal("100"), decimal("0")),
        Row(1004L, 1L, 10L, decimal("100"), decimal("0"))
      ))
  )

  private lazy val query = {
    val source = Source.fromInputStream(
      getClass.getResourceAsStream("/q8-early-broadcast/query.sql"),
      "UTF-8")
    try source.mkString
    finally source.close()
  }

  private val expected = Seq(Row(1995, decimal("0.718750")), Row(1996, decimal("0.250000")))

  test("Q8 broadcast configuration defaults off") {
    assume(extensionAvailable, "Requires the Velox backend")
    val configClass = Class.forName("org.apache.gluten.config.VeloxConfig$")
    val config = configClass.getField("MODULE$").get(null)
    val entry = configClass.getMethod("Q8_EARLY_PART_BROADCAST_ENABLED").invoke(config)
    assert(entry.getClass.getMethod("defaultValue").invoke(entry) == Some(false))
    assert(entry.getClass.getMethod("key").invoke(entry) == key)
  }

  test("full Q8 semantics, actual broadcast activation, and rejected shapes") {
    assume(extensionAvailable, "Requires the Velox backend")
    withTempPath {
      dir =>
        withTempView(tables.map(_._1): _*) {
          tables.foreach {
            case (name, schema, rows) =>
              val path = new java.io.File(dir, name).toString
              spark.createDataFrame(
                spark.sparkContext.parallelize(rows),
                StructType.fromDDL(schema))
                .write.parquet(path)
              spark.read.parquet(path).createOrReplaceTempView(name)
          }

          def runCase(
              sqlText: String,
              answer: Seq[Row],
              shouldMatch: Boolean,
              aqe: Boolean = false,
              preserveMerge: Boolean = false): Unit = {
            withSQLConf("spark.sql.adaptive.enabled" -> aqe.toString) {
              Seq(false, true, false).foreach {
                enabled =>
                  withSQLConf(key -> enabled.toString) {
                    val frame = sql(sqlText)
                    val optimized = frame.queryExecution.optimizedPlan
                    val hints = optimized.collect {
                      case j: Join => Seq(j.hint.leftHint, j.hint.rightHint).flatten
                          .count(_.strategy.contains(BROADCAST))
                    }.sum
                    assert(hints == (if (enabled && shouldMatch) 1 else 0), optimized.toString)
                    checkAnswer(frame, answer)
                    if (enabled && shouldMatch) {
                      val physical = frame.queryExecution.executedPlan.toString
                      assert(
                        physical.split("\n").exists {
                          line =>
                            line.contains("BroadcastHashJoin") && line.contains("p_partkey") &&
                            line.contains("l_partkey")
                        },
                        physical)
                    }
                    if (preserveMerge) assert(optimized.toString.contains("strategy=merge"))
                  }
              }
            }
          }

          Seq(false, true).foreach(aqe => runCase(query, expected, shouldMatch = true, aqe = aqe))
          runCase(query.replace(selectedType, "OTHER"), Seq(Row(1995, decimal("1.000000"))), false)
          runCase(query.replace("as mkt_share", "as share"), expected, false)
          runCase(
            query.replace("p_partkey = l_partkey", "p_partkey <=> l_partkey"),
            Seq(Row(1995, decimal("0.785714")), Row(1996, decimal("0.250000"))),
            false)
          val hinted = query.replace("\t\tselect\n", "\t\tselect /*+ MERGE(part) */\n")
          assert(hinted != query)
          runCase(hinted, expected, false, preserveMerge = true)
          runCase("select count(*) as n from part", Seq(Row(7L)), false)
          val emptyPath = new java.io.File(dir, "part-empty-selection").toString
          spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(1L, "OTHER"), Row(null, "OTHER"))),
            StructType.fromDDL("p_partkey long, p_type string")).write.parquet(emptyPath)
          spark.read.parquet(emptyPath).createOrReplaceTempView("part")
          runCase(query, Seq.empty, true)
        }
    }
  }
}
