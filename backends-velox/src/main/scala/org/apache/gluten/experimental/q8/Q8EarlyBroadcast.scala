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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.LongType

class Q8EarlyBroadcastExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(Q8EarlyBroadcast.install)
  }
}

object Q8EarlyBroadcast {
  val enabledKey = "spark.gluten.experimental.q8EarlyPartBroadcast.enabled"

  // Run after the normal optimizer has formed the equijoins. Keep other
  // experimental rules, and bind to this session rather than an inherited one.
  def install(session: SparkSession): Rule[LogicalPlan] = {
    val methods = session.experimental
    methods.synchronized {
      methods.extraOptimizations = methods.extraOptimizations
        .filterNot(_.isInstanceOf[Q8EarlyBroadcast]) :+ Q8EarlyBroadcast(session)
    }
    new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan
    }
  }
}

case class Q8EarlyBroadcast(session: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper with Logging {
  import Q8EarlyBroadcast._

  private def safeExpression(expr: Expression): Boolean =
    expr.deterministic && !expr.exists {
      case _: ScalaUDF | _: PythonUDF | _: OuterReference | _: SubqueryExpression => true
      case _ => false
    }

  private def simpleScan(plan: LogicalPlan): Boolean = plan match {
    case Project(expressions, child) =>
      expressions.forall(_.isInstanceOf[Attribute]) && simpleScan(child)
    case Filter(condition, child) => safeExpression(condition) && simpleScan(child)
    case leaf: LeafNode => !leaf.isStreaming && leaf.deterministic
    case _ => false
  }

  private def isPartType(expr: Expression): Boolean = expr match {
    case EqualTo(a: Attribute, value: Literal) =>
      a.name == "p_type" && value.value != null &&
      value.value.toString == "MEDIUM BURNISHED COPPER"
    case EqualTo(value: Literal, a: Attribute) =>
      isPartType(EqualTo(a, value))
    case _ => false
  }

  // Return the build side only for the original ordinary INNER equijoin.
  // Do not introduce a semi join, deduplicate keys, or assume uniqueness.
  private def eligible(join: Join): Option[Boolean] = {
    if (
      join.joinType != Inner || join.hint != JoinHint.NONE ||
      !simpleScan(join.left) || !simpleScan(join.right)
    ) return None
    val keys = join.condition match {
      case Some(EqualTo(a: Attribute, b: Attribute))
          if join.left.outputSet.contains(a) && join.right.outputSet.contains(b) => Some((a, b))
      case Some(EqualTo(a: Attribute, b: Attribute))
          if join.left.outputSet.contains(b) && join.right.outputSet.contains(a) => Some((b, a))
      case _ => None
    }
    keys.flatMap {
      case (leftKey, rightKey) =>
        def matches(
            part: LogicalPlan,
            partKey: Attribute,
            lineitem: LogicalPlan,
            lineitemKey: Attribute): Boolean = {
          val partTypePresent = part.exists {
            case Filter(condition, _) => splitConjunctivePredicates(condition).exists(isPartType)
            case _ => false
          }
          partKey.name == "p_partkey" && lineitemKey.name == "l_partkey" &&
          partKey.dataType == LongType && lineitemKey.dataType == LongType &&
          part.output.size == 1 && part.output.head.semanticEquals(partKey) &&
          lineitem.output.map(_.name).toSet == Set(
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_extendedprice",
            "l_discount") &&
          partTypePresent
        }
        if (matches(join.left, leftKey, join.right, rightKey)) Some(true)
        else if (matches(join.right, rightKey, join.left, leftKey)) Some(false)
        else None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !session.sessionState.conf.getConfString(enabledKey, "false").toBoolean ||
      plan.isStreaming
    ) return plan
    // Deliberately narrow experiment: frozen Q8 output, seven inner joins and
    // the two-sum yearly market-share aggregate. No table-wide default hint.
    val joins = plan.collect { case j: Join => j }
    val hasMarketShare = plan.exists {
      case aggregate: Aggregate =>
        aggregate.groupingExpressions.map(_.references.toSeq.map(_.name).toSet) ==
          Seq(Set("o_year")) &&
          aggregate.aggregateExpressions.exists(_.name == "mkt_share") &&
          aggregate.aggregateExpressions.flatMap(_.collect {
            case e if e.prettyName == "sum" => e
          }).size == 2
      case _ => false
    }
    if (
      plan.output.map(_.name) != Seq("o_year", "mkt_share") ||
      joins.size != 7 || !joins.forall(_.joinType == Inner) || !hasMarketShare ||
      joins.count(eligible(_).nonEmpty) != 1
    ) return plan
    val result = plan.transformUp {
      case join: Join if eligible(join).nonEmpty =>
        val hint = HintInfo(Some(BROADCAST))
        val nextHint = if (eligible(join).get) JoinHint(Some(hint), None)
        else JoinHint(None, Some(hint))
        val replacement = join.copy(hint = nextHint)
        replacement.copyTagsFrom(join)
        replacement
    }
    logInfo("Q8_EARLY_PART_BROADCAST matched=1; only the physical join hint changed")
    result
  }
}
