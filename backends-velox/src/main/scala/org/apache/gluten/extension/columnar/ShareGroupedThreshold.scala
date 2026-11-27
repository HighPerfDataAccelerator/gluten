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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_COL, ShuffleExchangeExec}
import org.apache.spark.sql.types.{ByteType, DecimalType, IntegerType, LongType, ShortType}

// Keep both consumers' input schemas identical through CTE inlining and column pruning.
// The scalar consumes only the measure, while the outer aggregate also needs grouping keys.
case class SharedGroupingInput(
    child: LogicalPlan,
    partitionKeys: Seq[Attribute],
    requiredColumns: Seq[Attribute]) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): SharedGroupingInput =
    copy(child = newChild)
}

case class PlanSharedGroupingInput(session: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SharedGroupingInput(child, keys, _) =>
      val distribution = HashPartitioning(keys, session.sessionState.conf.numShufflePartitions)
      Seq(ShuffleExchangeExec(distribution, planLater(child), REPARTITION_BY_COL))
    case _ => Nil
  }
}

/** Share a grouped join between a filtered rollup and its unfiltered scalar AVG threshold. */
object ShareGroupedThreshold extends Rule[LogicalPlan] {
  private def underlying(plan: LogicalPlan): LogicalPlan = plan match {
    case a: SubqueryAlias => underlying(a.child)
    case other => other
  }

  private def ref(plan: LogicalPlan): Option[CTERelationRef] = underlying(plan) match {
    case r: CTERelationRef => Some(r)
    case _ => None
  }

  private def measures(a: Aggregate): Seq[AggregateExpression] =
    a.aggregateExpressions.flatMap(_.collect { case e: AggregateExpression => e })

  private def sumInput(a: Aggregate): Option[Attribute] = measures(a) match {
    case Seq(e) if e.filter.isEmpty && !e.isDistinct =>
      e.aggregateFunction match {
        case sum: Sum => Some(sum.child).collect { case k: Attribute => k }
        case _ => None
      }
    case _ => None
  }

  // Both consumers already evaluate this exact sum. Avoid materializing unrelated expressions
  // or introducing a new reduction order for approximate numerics.
  private def measureOrdinal(a: Aggregate): Option[Int] = {
    val outputs = a.aggregateExpressions
    val keys = a.groupingExpressions
    val idx = outputs.indexWhere(_.exists(_.isInstanceOf[AggregateExpression]))
    val valid = idx >= 0 && keys.nonEmpty && keys.forall(_.isInstanceOf[Attribute]) &&
      outputs.zipWithIndex.forall {
        case (Alias(_: AggregateExpression, _), i) if i == idx => true
        case (k: Attribute, _) => keys.exists(_.semanticEquals(k))
        case _ => false
      }
    val exact = sumInput(a).exists(_.dataType match {
      case _: DecimalType | ByteType | ShortType | IntegerType | LongType => true
      case _ => false
    })
    if (valid && exact) Some(idx) else None
  }

  private def groupedJoin(a: Aggregate): Boolean = {
    val joins = a.child.collect { case j: Join => j }
    joins.size >= 2 && joins.forall(_.joinType == Inner) && !a.exists {
      node =>
        val supported = node == a || node.children.isEmpty || (node match {
          case _: Project | _: Filter | _: SubqueryAlias | _: View | _: Join => true
          case _ => false
        })
        !supported || node.expressions.exists(
          e =>
            !e.deterministic ||
              SubqueryExpression.hasSubquery(e) || e.exists(_.isInstanceOf[OuterReference]))
    }
  }

  private def scalarMatches(having: Expression, id: Long, index: Int): Boolean =
    having.collect { case q: SubqueryExpression => q } match {
      case Seq(q: ScalarSubquery) if !q.isCorrelated =>
        underlying(q.plan) match {
          case a: Aggregate if a.groupingExpressions.isEmpty && a.aggregateExpressions.size == 1 =>
            (ref(a.child), measures(a)) match {
              case (Some(r), Seq(e)) if r.cteId == id && !e.isDistinct && e.filter.isEmpty =>
                e.aggregateFunction match {
                  case avg: Average => avg.child.semanticEquals(r.output(index))
                  case _ => false
                }
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }

  private def sharingKeys(
      having: Expression,
      rollup: Aggregate,
      defs: Map[Long, LogicalPlan]): Option[(Long, Seq[Int])] = for {
    filtered <- underlying(rollup.child) match {
      case f: Filter if f.condition.deterministic && !SubqueryExpression.hasSubquery(f.condition) =>
        Some(f)
      case _ => None
    }
    main <- ref(filtered.child)
    grouped <- defs.get(main.cteId).collect { case a: Aggregate if groupedJoin(a) => a }
    idx <- measureOrdinal(grouped)
    sum <- sumInput(rollup) if sum.semanticEquals(main.output(idx))
    if filtered.condition.references.nonEmpty && !filtered.condition.references.contains(sum)
    if having.deterministic && scalarMatches(having, main.cteId, idx)
    keys = rollup.groupingExpressions.map(k => main.output.indexWhere(_.semanticEquals(k)))
    if keys.nonEmpty && keys.forall(i => i >= 0 && i != idx)
  } yield main.cteId -> keys

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !conf.exchangeReuseEnabled || !conf.adaptiveExecutionEnabled ||
      !new VeloxConfig(conf).shareGroupedThresholdEnabled || plan.isStreaming
    ) return plan
    plan.transformUp {
      case w: WithCTE if w.resolved && !w.plan.exists(_.isInstanceOf[SharedGroupingInput]) =>
        val defs = w.cteDefs.map(d => d.id -> underlying(d.child)).toMap
        val keys = w.plan.collect {
          case Filter(having, a: Aggregate) => sharingKeys(having, a, defs)
        }.flatten.toMap
        if (keys.isEmpty) w
        else {
          val refs = w.collectWithSubqueries { case r: CTERelationRef => r.cteId }
          val matched = keys.filter { case (id, _) => refs.count(_ == id) == 2 }
          w.copy(plan = w.plan.transformUpWithSubqueries {
            case r: CTERelationRef if matched.contains(r.cteId) =>
              SharedGroupingInput(r, matched(r.cteId).map(r.output), r.output)
          })
        }
    }
  }
}
