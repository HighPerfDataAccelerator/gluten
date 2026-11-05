/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.experimental

import java.util.concurrent.atomic.AtomicLong
import org.apache.gluten.execution.ShuffledHashJoinExecTransformer
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{LeftSemi, LeftAnti}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.types.{IntegerType, LongType}

/** Register before GlutenSessionExtensions. Both independent flags default off. */
class ExistenceBuildLeftExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar { session => new ColumnarRule {
      override def postColumnarTransitions: Rule[SparkPlan] = new ExistenceBuildLeft(session)
    }}
  }
}

object ExistenceBuildLeft {
  val semiKey = "spark.gluten.experimental.semiJoinBuildLeft.enabled"
  val antiKey = "spark.gluten.experimental.antiJoinBuildLeft.enabled"
  private val semiMoved = new AtomicLong()
  private val antiMoved = new AtomicLong()
  private val rejected = new AtomicLong()
  def resetStats(): Unit = { semiMoved.set(0); antiMoved.set(0); rejected.set(0) }
  def stats(): String = s"{\"semi\":${semiMoved.get()},\"anti\":${antiMoved.get()},\"rejected\":${rejected.get()}}"
}

/** Private factorial experiment: preserve logical joins and change only native
 * build-side selection. No general cost policy or additive speedup is assumed.
 */
class ExistenceBuildLeft(session: SparkSession) extends Rule[SparkPlan] {
  import ExistenceBuildLeft._
  private def supported(j: ShuffledHashJoinExecTransformer): Boolean = {
    val keySupported = (j.leftKeys, j.rightKeys) match {
      case (Seq(a: Attribute), Seq(b: Attribute)) =>
        a.dataType == b.dataType && (a.dataType == IntegerType || a.dataType == LongType) &&
          j.left.outputSet.contains(a) && j.right.outputSet.contains(b)
      case _ => false
    }
    keySupported && !j.isSkewJoin && j.condition.exists {
      case Not(EqualTo(a: Attribute, b: Attribute)) if a.dataType == b.dataType &&
          (a.dataType == IntegerType || a.dataType == LongType) =>
        (j.left.outputSet.contains(a) && j.right.outputSet.contains(b)) ||
          (j.right.outputSet.contains(a) && j.left.outputSet.contains(b))
      case _ => false
    }
  }
  override def apply(plan: SparkPlan): SparkPlan = {
    val semi = session.sessionState.conf.getConfString(semiKey, "false").toBoolean
    val anti = session.sessionState.conf.getConfString(antiKey, "false").toBoolean
    if (!semi && !anti) return plan
    plan.transformUp {
      case j: ShuffledHashJoinExecTransformer if
          ((semi && j.joinType == LeftSemi) || (anti && j.joinType == LeftAnti)) &&
          j.buildSide == BuildRight && supported(j) =>
        val changed = j.copy(buildSide = BuildLeft)
        changed.copyTagsFrom(j)
        if (changed.doValidate().ok()) {
          require(changed.output == j.output)
          require(changed.outputPartitioning == j.outputPartitioning)
          if (j.joinType == LeftSemi) semiMoved.incrementAndGet() else antiMoved.incrementAndGet()
          changed
        } else {
          rejected.incrementAndGet()
          j
        }
    }
  }
}
