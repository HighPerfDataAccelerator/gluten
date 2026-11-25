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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

/** Share the rows of a join used by a grouped SUM and by its scalar SUM threshold. */
private[columnar] object ShareScalarSumInput extends PredicateHelper {
  private def sumOf(aggregate: Aggregate): Option[Sum] =
    aggregate.aggregateExpressions.flatMap(_.collect { case e: AggregateExpression => e }) match {
      case Seq(e) if !e.isDistinct && e.filter.isEmpty =>
        e.aggregateFunction match {
          case s: Sum
              if s.child.dataType.isInstanceOf[DecimalType] && s.child.references.nonEmpty =>
            Some(s)
          case _ => None
        }
      case _ => None
    }

  private def joined(plan: LogicalPlan): Boolean = {
    val joins = plan.collect { case j: Join => j }
    joins.size >= 2 && joins.forall(j => j.joinType == Inner && j.hint == JoinHint.NONE) &&
    !plan.exists {
      node =>
        val supported = node.children.isEmpty || (node match {
          case _: Project | _: Filter | _: SubqueryAlias | _: View | _: Join => true
          case _ => false
        })
        !supported || node.expressions.exists(
          e =>
            !e.deterministic || SubqueryExpression.hasSubquery(e) || e.exists(
              _.isInstanceOf[OuterReference]))
    }
  }

  // Every input column required outside SUM must survive the projection onto the group key.
  private def usesOnly(aggregate: Aggregate, sum: Sum, keys: Seq[Attribute]): Boolean =
    aggregate.aggregateExpressions.forall {
      expr =>
        expr.deterministic && !SubqueryExpression.hasSubquery(expr) &&
        expr.transformUp {
          case e: AggregateExpression if e.aggregateFunction.semanticEquals(sum) =>
            Literal.create(null, e.dataType)
        }
          .references.subsetOf(AttributeSet(keys))
    }

  private def shared(aggregate: Aggregate, key: Attribute, sum: Sum, threshold: Long): Aggregate = {
    val equalities = aggregate.child.collect {
      case f: Filter => splitConjunctivePredicates(f.condition)
      case j: Join => j.condition.toSeq.flatMap(splitConjunctivePredicates)
    }.flatten.collect { case EqualTo(a: Attribute, b: Attribute) => a -> b }
    def equijoin(left: LogicalPlan, right: LogicalPlan): Boolean = equalities.exists {
      case (a, b) => (left.outputSet.contains(a) && right.outputSet.contains(b)) ||
        (right.outputSet.contains(a) && left.outputSet.contains(b))
    }
    val conf = SQLConf.get
    val buildBudget = BigInt(threshold) * conf.numShufflePartitions
    val input = aggregate.child.transformUp {
      case j @ Join(left, right, Inner, _, JoinHint.NONE)
          if threshold > 0 && equijoin(left, right) &&
            left.stats.sizeInBytes != conf.defaultSizeInBytes &&
            right.stats.sizeInBytes != conf.defaultSizeInBytes &&
            right.stats.sizeInBytes > threshold && right.stats.sizeInBytes <= buildBudget &&
            left.stats.sizeInBytes >= right.stats.sizeInBytes * 4 =>
        j.copy(hint = JoinHint(None, Some(HintInfo(strategy = Some(SHUFFLE_HASH)))))
    }
    val value = Alias(sum.child, "_shared_sum_input")()
    val columns = Project(Seq(key, value), input)
    val exchange = SharedGroupingInput(columns, Seq(key), columns.output)
    aggregate.copy(
      aggregateExpressions = aggregate.aggregateExpressions.map {
        e =>
          e.transformUp {
            case s: Sum if s.semanticEquals(sum) => s.withNewChildren(Seq(value.toAttribute))
          }.asInstanceOf[NamedExpression]
      },
      child = exchange
    )
  }

  private def rewrite(
      filter: Filter,
      aggregate: Aggregate,
      broadcastThreshold: Long): Option[LogicalPlan] = for {
    query <- filter.condition.collect { case q: SubqueryExpression => q } match {
      case Seq(q: ScalarSubquery) if !q.isCorrelated => Some(q)
      case _ => None
    }
    scalar <- query.plan match {
      case a: Aggregate if a.groupingExpressions.isEmpty && a.aggregateExpressions.size == 1 =>
        Some(a)
      case _ => None
    }
    key <- aggregate.groupingExpressions match {
      case Seq(k: Attribute) => Some(k)
      case _ => None
    }
    if filter.condition.deterministic && joined(aggregate.child)
    if aggregate.child.exists(n => n.children.isEmpty && n.stats.sizeInBytes > broadcastThreshold)
    outer <- sumOf(aggregate) if usesOnly(aggregate, outer, Seq(key))
    if aggregate.child.sameResult(scalar.child)
    if aggregate.child.output.map(_.dataType) == scalar.child.output.map(_.dataType)
    index = aggregate.child.output.indexWhere(_.semanticEquals(key)) if index >= 0
    inner <- sumOf(scalar) if usesOnly(scalar, inner, Nil)
    if BindReferences.bindReference(outer.child, aggregate.child.output).semanticEquals(
      BindReferences.bindReference(inner.child, scalar.child.output))
  } yield {
    val replacement =
      query.withNewPlan(shared(scalar, scalar.child.output(index), inner, broadcastThreshold))
    Filter(
      filter.condition.transformUp {
        case q: ScalarSubquery if q.exprId == query.exprId => replacement
      },
      shared(aggregate, key, outer, broadcastThreshold))
  }

  def apply(plan: LogicalPlan, broadcastThreshold: Long): LogicalPlan = plan.transformUp {
    case f @ Filter(_, a: Aggregate) if f.resolved => rewrite(f, a, broadcastThreshold).getOrElse(f)
  }
}
