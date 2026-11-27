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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DecimalType, IntegerType, LongType, ShortType}

/** Materialize the one-column membership result before Spark inlines the defining CTE. */
case class SharedMembershipKeys(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): SharedMembershipKeys =
    copy(child = newChild)
}

case class PlanMembershipKeys(session: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SharedMembershipKeys(child) =>
      val distribution =
        HashPartitioning(child.output, session.sessionState.conf.numShufflePartitions)
      // AQE can share this exchange even when the grouped child already has this partitioning.
      Seq(ShuffleExchangeExec(distribution, planLater(child), ENSURE_REQUIREMENTS))
    case _ => Nil
  }
}

object ShareGroupedMembershipKeys extends Rule[LogicalPlan] {
  private def bare(plan: LogicalPlan): LogicalPlan = plan match {
    case alias: SubqueryAlias => bare(alias.child)
    case p => p
  }

  // ExprIds change at each CTE reference. Compare the projected key by ordinal.
  private def projectedKey(plan: LogicalPlan): Option[(Long, Int)] = bare(plan) match {
    case Project(Seq(k: Attribute), child) => bare(child) match {
        case r: CTERelationRef =>
          val i = r.output.indexWhere(_.semanticEquals(k))
          if (i >= 0) Some(r.cteId -> i) else None
        case _ => None
      }
    case _ => None
  }

  private def exact(plan: LogicalPlan): Boolean = !plan.exists {
    n =>
      val supported = n match {
        case _: CTERelationRef => false
        case j: Join => j.joinType == Inner
        case _: LeafNode | _: View | _: SubqueryAlias | _: Aggregate | _: Project | _: Filter =>
          true
        case _ => false
      }
      !supported || n.expressions.exists(
        e =>
          !e.deterministic || e.exists {
            case _: OuterReference | _: SubqueryExpression => true
            case a: AggregateExpression => a.aggregateFunction match {
                case _: Count => false
                case _: Sum => !a.dataType.isInstanceOf[DecimalType]
                case _: Max | _: Min => a.dataType match {
                    case ByteType | ShortType | IntegerType | LongType | _: DecimalType => false
                    case _ => true
                  }
                case _ => true
              }
            case _ => false
          })
  }

  private def uncorrelatedThreshold(
      scalar: ScalarSubquery,
      defs: Map[Long, LogicalPlan]): Boolean = {
    val target = projectedKey(scalar.plan) match {
      case Some((id, index)) if index == 0 => defs.get(id)
      case None => Some(bare(scalar.plan))
      case _ => None
    }
    !scalar.isCorrelated && target.exists {
      case a: Aggregate if a.groupingExpressions.isEmpty && a.aggregateExpressions.size == 1 =>
        exact(a) && a.aggregateExpressions.head.exists {
          case agg: AggregateExpression => agg.aggregateFunction.isInstanceOf[Max] ||
            agg.aggregateFunction.isInstanceOf[Min]
          case _ => false
        }
      case _ => false
    }
  }

  // A broadcast dimension may already let both consumers reuse the whole filtered broadcast.
  // A shuffle of its keys adds work in that case.
  private def shuffledJoin(plan: LogicalPlan, threshold: Long): Boolean = {
    val scans = plan.collectLeaves().filter(_.output.nonEmpty)
    scans.count {
      leaf =>
        val bytes = leaf.stats.sizeInBytes
        bytes > threshold && bytes != conf.defaultSizeInBytes
    } >= 2 && plan.exists(_.isInstanceOf[Join])
  }

  private def needsDistinct(
      plan: LogicalPlan,
      key: Int,
      defs: Map[Long, LogicalPlan],
      config: VeloxConfig,
      threshold: Long): Option[Boolean] = plan match {
    case Filter(pred, a: Aggregate)
        if pred.deterministic && exact(a) && shuffledJoin(a, threshold) =>
      val field = a.aggregateExpressions(key) match {
        case alias: Alias => alias.child
        case other => other
      }
      val measures = a.aggregateExpressions.filter(
        e =>
          pred.references.contains(e.toAttribute) && e.exists(_.isInstanceOf[AggregateExpression]))
      val scalar = pred.collect { case q: SubqueryExpression => q }
      val eligible = scalar match {
        case Seq(q: ScalarSubquery) =>
          config.reuseScalarHavingKeysEnabled && conf.subqueryReuseEnabled &&
          uncorrelatedThreshold(q, defs) && measures.exists(_.exists(_.isInstanceOf[Sum]))
        case Seq() => config.reuseFilteredFinalAggregateEnabled &&
          measures.exists(_.exists(_.isInstanceOf[Count]))
        case _ => false
      }
      if (eligible && a.groupingExpressions.exists(_.semanticEquals(field))) {
        Some(a.groupingExpressions.size > 1)
      } else None
    case _ => None
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val config = new VeloxConfig(conf)
    if (
      !conf.adaptiveExecutionEnabled || !conf.exchangeReuseEnabled ||
      (!config.reuseFilteredFinalAggregateEnabled && !config.reuseScalarHavingKeysEnabled) ||
      plan.isStreaming
    ) return plan
    val threshold = math.max(
      conf.autoBroadcastJoinThreshold,
      conf.getConf(
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD).getOrElse(conf.autoBroadcastJoinThreshold))
    plan.transformUp {
      case w: WithCTE if w.resolved && w.plan.collectWithSubqueries {
            case x: SharedMembershipKeys => x
          }.isEmpty =>
        val uses = w.plan.collectWithSubqueries {
          case n =>
            n.expressions.flatMap(_.collect {
              case InSubquery(values, q) if values.size == 1 && !q.isCorrelated =>
                projectedKey(q.plan)
            }.flatten)
        }.flatten
        val refs = w.collectWithSubqueries { case r: CTERelationRef => r.cteId }
        val defs = w.cteDefs.map(d => d.id -> bare(d.child)).toMap
        val matches = uses.distinct.flatMap {
          entry =>
            val (id, i) = entry
            val n = uses.count(_ == entry)
            if (n >= 2 && refs.count(_ == id) == n) {
              defs.get(id).flatMap(needsDistinct(_, i, defs, config, threshold)).map(entry -> _)
            } else None
        }.toMap
        if (matches.isEmpty) w
        else w.copy(plan = w.plan.transformUpWithSubqueries {
          case proj: Project if projectedKey(proj).exists(matches.contains) =>
            // IN and NOT IN depend only on membership, including presence of a null key.
            SharedMembershipKeys(if (matches(projectedKey(proj).get)) Distinct(proj) else proj)
        })
    }
  }
}
