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
package org.apache.gluten.expression

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class ExpressionMappingsCacheSuite extends SparkFunSuite with SharedSparkSession {
  test("changing the session blacklist preserves both expression maps") {
    val key = GlutenConfig.EXPRESSION_BLACK_LIST.key
    val regexp = GlutenConfig.FALLBACK_REGEXP_EXPRESSIONS.key
    withSQLConf(key -> "", regexp -> "false") {
      val all = ExpressionMappings.expressionsMap
      assert(ExpressionMappings.blacklistExpressionMap.isEmpty)
      val blocked =
        Set("add", "rlike", "regexp_replace", "regexp_extract", "regexp_extract_all", "split")
      val (disabled, enabled) = all.partition { case (_, name) => blocked.contains(name) }
      assert(disabled.nonEmpty && enabled.nonEmpty)
      withSQLConf(key -> " ADD, add ", regexp -> "true") {
        assert(ExpressionMappings.blacklistExpressionMap == disabled)
        assert(ExpressionMappings.expressionsMap == enabled)
      }
      assert(ExpressionMappings.expressionsMap == all)
      assert(ExpressionMappings.blacklistExpressionMap.isEmpty)
    }
  }
}
