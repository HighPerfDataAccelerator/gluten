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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DecimalType

/** Reuse a checked decimal subtotal when the same grouped fact is used for membership and SUM. */
case class ReuseMembershipSum(session: SparkSession) extends SparkStrategy with PredicateHelper {
  // The equality joins discard null keys. Every other row and filter must agree on both scans.
  private def scan(p: LogicalPlan, key: Attribute): Option[LogicalRelation] = p match {
    case Project(list, c) if list.forall(_.isInstanceOf[Attribute]) => scan(c, key)
    case Filter(IsNotNull(a: Attribute), c) if a.semanticEquals(key) => scan(c, key)
    case x: RepartitionByExpression if x.partitionExpressions.forall(_.semanticEquals(key)) =>
      scan(x.child, key)
    case r: LogicalRelation if !r.isStreaming => Some(r)
    case _ => None
  }

  private def exactDecimal(s: Sum): Boolean = s.child.dataType match {
    case d: DecimalType => d.precision > 8 && d.precision <= 18
    case _ => false
  }

  private case class QualifyingSubtotal(
      keys: LogicalPlan,
      filtered: LogicalPlan,
      key: Attribute,
      value: Attribute,
      check: CheckOverflowInSum)

  private def sameColumn(
      left: LogicalRelation,
      leftColumn: Expression,
      right: LogicalRelation,
      rightColumn: Expression): Boolean = {
    val index = left.output.indexWhere(_.semanticEquals(leftColumn))
    index >= 0 && right.output.lift(index).exists(_.semanticEquals(rightColumn))
  }

  private def joinsKeys(eq: EqualTo, left: Attribute, right: Attribute): Boolean =
    (eq.left.semanticEquals(left) && eq.right.semanticEquals(right)) ||
      (eq.left.semanticEquals(right) && eq.right.semanticEquals(left))

  private def payload(
      fact: LogicalPlan,
      fk: Attribute,
      measure: Attribute,
      keys: LogicalPlan,
      outer: Sum): Option[QualifyingSubtotal] = keys match {
    case Project(Seq(key: Attribute), filtered @ Filter(condition, a: Aggregate))
        if keys.deterministic && a.groupingExpressions.size == 1 && a.groupingExpressions.head.semanticEquals(
          key) =>
      for {
        raw <- scan(fact, fk)
        creation <- scan(a.child, key) if raw.sameResult(creation)
        if sameColumn(raw, fk, creation, key)
        al <- a.aggregateExpressions.collectFirst {
          case x @ Alias(AggregateExpression(s: Sum, Complete, false, None, _), _) if
                s.child.isInstanceOf[Attribute] && s.withNewChildren(
                  Seq(outer.child)).semanticEquals(outer) &&
                  s.child.dataType == measure.dataType && s.dataType == outer.dataType &&
                  sameColumn(raw, measure, creation, s.child) => x
        }
        value = al.toAttribute
        if splitConjunctivePredicates(condition).exists {
          case b: BinaryComparison => b.children.exists(_.semanticEquals(value)) &&
            b.children.exists {
              case l: Literal => l.value != null
              case _ => false
            } && !b.isInstanceOf[EqualNullSafe]
          case _ => false
        }
        check <- outer.evaluateExpression.collectFirst { case c: CheckOverflowInSum => c }
      } yield QualifyingSubtotal(keys, filtered, key, value, check)
    case _ => None
  }

  // Each dimension row keeps its multiplicity and carries the subtotal for its join key.
  private def bind(
      plan: LogicalPlan,
      k: Attribute,
      p: QualifyingSubtotal,
      out: Alias): Option[LogicalPlan] = plan match {
    case j @ Join(l, r, LeftSemi, Some(eq: EqualTo), JoinHint.NONE)
        if l.outputSet.intersect(p.filtered.outputSet).isEmpty && r.output.size == 1 &&
          joinsKeys(eq, k, r.output.head) && r.sameResult(p.keys) =>
      val inner = Join(l, p.filtered, Inner, Some(EqualTo(k, p.key)), JoinHint.NONE)
      Some(Project(j.output :+ out, inner))
    case proj: Project
        if proj.projectList.forall(_.isInstanceOf[Attribute]) && proj.outputSet.contains(k) =>
      bind(proj.child, k, p, out).map(c => Project(proj.projectList :+ out.toAttribute, c))
    case j @ Join(l, r, Inner, Some(cond), JoinHint.NONE)
        if cond.deterministic && l.outputSet.contains(k) =>
      bind(l, k, p, out).map(c => j.copy(left = c))
    case j @ Join(l, r, Inner, Some(cond), JoinHint.NONE)
        if cond.deterministic && r.outputSet.contains(k) =>
      bind(r, k, p, out).map(c => j.copy(right = c))
    case _ => None
  }

  private def rewrite(
      a: Aggregate,
      left: LogicalPlan,
      dimKey: Attribute,
      fact: LogicalPlan,
      factKey: Attribute,
      keys: LogicalPlan): Option[LogicalPlan] = {
    val group = a.groupingExpressions
    if (
      !group.exists(_.semanticEquals(dimKey)) || !group.forall {
        case attr: Attribute => left.outputSet.contains(attr)
        case _ => false
      }
    ) return None
    val measures = a.aggregateExpressions.filterNot {
      case attr: Attribute => group.exists(_.semanticEquals(attr))
      case _ => false
    }
    measures match {
      case Seq(alias @ Alias(AggregateExpression(s: Sum, Complete, false, None, _), _))
          if exactDecimal(s) && s.child.isInstanceOf[Attribute] && fact.outputSet.contains(
            s.child.asInstanceOf[Attribute]) =>
        for {
          p <- payload(fact, factKey, s.child.asInstanceOf[Attribute], keys, s)
          carried = Alias(p.value, "grouped_subtotal")()
          child <- bind(left, dimKey, p, carried)
        } yield {
          // Every row in this outer group has the same key and the same non-null subtotal.
          // Repeated dimension rows must each contribute it. Restore SUM's original precision
          // and overflow behavior with its own final check after summing the wider values.
          val total =
            s.withNewChildren(Seq(carried.toAttribute)).asInstanceOf[Sum].toAggregateExpression()
          val finalValue = p.check.withNewChildren(Seq(total))
          val exprs = a.aggregateExpressions.map(
            e =>
              if (e.exprId == alias.exprId)
                alias.withNewChildren(Seq(finalValue)).asInstanceOf[NamedExpression]
              else e)
          a.copy(aggregateExpressions = exprs, child = child)
        }
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case a: Aggregate if !a.isStreaming =>
      val child = a.child match {
        case Project(list, c) if list.forall(_.isInstanceOf[Attribute]) => c
        case p => p
      }
      child match {
        case Join(
              left,
              Join(fact, keys, LeftSemi, Some(membership: EqualTo), JoinHint.NONE),
              Inner,
              Some(EqualTo(first: Attribute, second: Attribute)),
              JoinHint.NONE) if left.deterministic && keys.output.size == 1 =>
          val (dimKey, factKey) =
            if (left.outputSet.contains(first)) (first, second) else (second, first)
          if (
            left.outputSet.contains(dimKey) && fact.outputSet.contains(factKey) &&
            joinsKeys(membership, factKey, keys.output.head) &&
            new VeloxConfig(session.sessionState.conf).reuseGroupedDecimalSumEnabled
          ) {
            rewrite(a, left, dimKey, fact, factKey, keys).map(planLater).toSeq
          } else Nil
        case _ => Nil
      }
    case _ => Nil
  }
}
