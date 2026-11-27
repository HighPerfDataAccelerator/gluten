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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/** Count groups on each side of a keyed inner join before multiplying their multiplicities. */
case class FactorGroupedJoinCounts(session: SparkSession) extends SparkStrategy {
  private def unwrap(plan: LogicalPlan): LogicalPlan = plan match {
    case Project(cols, child) if cols.forall(_.isInstanceOf[Attribute]) => unwrap(child)
    case p => p
  }

  private def pair(cond: Expression, left: LogicalPlan, right: LogicalPlan)
      : Option[(Attribute, Attribute)] = cond match {
    case EqualTo(a: Attribute, b: Attribute)
        if left.outputSet.contains(a) && right.outputSet.contains(b) =>
      Some(a -> b)
    case EqualTo(a: Attribute, b: Attribute)
        if left.outputSet.contains(b) && right.outputSet.contains(a) =>
      Some(b -> a)
    case _ => None
  }

  private def knownScanSize(plan: LogicalPlan): Option[BigInt] = {
    val sizes = plan.collectLeaves().map(_.stats.sizeInBytes)
    if (
      sizes.nonEmpty && sizes.forall(
        n => n > 0 && n != session.sessionState.conf.defaultSizeInBytes)
    ) {
      Some(sizes.max)
    } else None
  }

  private def factorCount(agg: Aggregate): Option[LogicalPlan] = {
    def isRowCount(expr: Expression): Boolean = expr match {
      case a: AggregateExpression if !a.isDistinct && a.filter.isEmpty =>
        a.aggregateFunction match {
          case Count(children) => children.nonEmpty && children.forall {
              case l: Literal => l.value != null
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
    def expression(n: NamedExpression): Expression = n match {
      case a: Alias => a.child
      case a => a
    }
    val groups = agg.groupingExpressions
    val outputs = agg.aggregateExpressions
    for {
      j <- unwrap(agg.child) match {
        case j @ Join(_, _, Inner, Some(_), JoinHint.NONE) if j.deterministic => Some(j)
        case _ => None
      }
      (leftKey, rightKey) <- pair(j.condition.get, j.left, j.right)
      if groups.forall(_.isInstanceOf[Attribute]) &&
        groups.exists(g => g.semanticEquals(leftKey) || g.semanticEquals(rightKey))
      if outputs.exists(n => isRowCount(expression(n))) && outputs.forall { n =>
        isRowCount(expression(n)) || groups.exists(_.semanticEquals(expression(n)))
      }
      keys = groups.map(_.asInstanceOf[Attribute])
      left = keys.filter(j.left.outputSet.contains)
      right = keys.filter(j.right.outputSet.contains)
      if left.size + right.size == keys.size && left.exists(!_.semanticEquals(leftKey)) &&
        right.exists(!_.semanticEquals(rightKey))
      fact <- knownScanSize(j.left)
      dim <- knownScanSize(j.right)
      if j.right.collectLeaves().size == 1 && fact > session.sessionState.conf.autoBroadcastJoinThreshold &&
        fact >= 4 * dim && dim > 0
    } yield {
      def counted(input: LogicalPlan, group: Seq[Attribute], key: Attribute, name: String)
          : (LogicalPlan, Attribute) = {
        val attrs = if (group.exists(_.semanticEquals(key))) group else group :+ key
        val count = Alias(Count(Seq(Literal(1))).toAggregateExpression(), name)()
        (Aggregate(attrs, attrs :+ count, input), count.toAttribute)
      }
      val (l, lc) = counted(j.left, left, leftKey, "_left_group_rows")
      val (r, rc) = counted(j.right, right, rightKey, "_right_group_rows")
      // Both counts are non-null for every matching inner-join group.
      val rows = Multiply(lc, rc)
      val result = outputs.map {
        case a: Alias if isRowCount(a.child) =>
          Alias(rows, a.name)(a.exprId, a.qualifier, Some(a.metadata))
        case other => other
      }
      Project(result, Join(l, r, Inner, j.condition, JoinHint.NONE))
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case a: Aggregate if !a.isStreaming && new VeloxConfig(session.sessionState.conf).factorGroupedJoinCounts =>
      factorCount(a).map(planLater).toSeq
    case _ => Nil
  }
}
