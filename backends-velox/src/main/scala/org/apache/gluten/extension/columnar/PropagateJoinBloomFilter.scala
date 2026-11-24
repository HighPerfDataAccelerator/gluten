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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALAR_SUBQUERY
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Transfer an existing membership predicate across an equality in a compound inner join when the
 * other side can apply it to its scan before an earlier join. Plan after logical runtime filter
 * creation so both sides can reuse the original scalar subquery.
 */
class PropagateJoinBloomFilter(session: SparkSession) extends SparkStrategy with PredicateHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case join @ Join(left, right, Inner, Some(condition), _)
        if join.containsPattern(SCALAR_SUBQUERY) && !join.isStreaming =>
      val conf = session.sessionState.conf
      if (!conf.subqueryReuseEnabled || !new VeloxConfig(conf).propagateJoinBloomFilterEnabled) {
        return Nil
      }
      val keys = splitConjunctivePredicates(condition).collect {
        case EqualTo(a: Attribute, b: Attribute)
            if a.dataType == b.dataType && (a.dataType == LongType || a.dataType == IntegerType) =>
          if (left.outputSet.contains(a) && right.outputSet.contains(b)) {
            Some(a -> b)
          } else if (left.outputSet.contains(b) && right.outputSet.contains(a)) {
            Some(b -> a)
          } else {
            None
          }
      }.flatten
      if (keys.size < 2) {
        return Nil
      }
      keys.iterator.flatMap {
        case (l, r) =>
          sourceFilter(right, r).flatMap(push(left, l, _, crossedJoin = false))
            .map(p => join.copy(left = p))
            .orElse(sourceFilter(left, l).flatMap(push(right, r, _, crossedJoin = false))
              .map(p => join.copy(right = p)))
      }.take(1).map(planLater).toList
    case _ => Nil
  }

  private def childKey(project: Project, key: Attribute): Option[Attribute] =
    project.projectList.find(_.exprId == key.exprId).collect {
      case a: Attribute => a
      case Alias(a: Attribute, _) => a
    }

  private def membership(condition: Expression, key: Attribute): Option[BloomFilterMightContain] =
    splitConjunctivePredicates(condition).collectFirst {
      case f @ BloomFilterMightContain(s: ScalarSubquery, XxHash64(Seq(a: Attribute), _))
          if a.exprId == key.exprId && !s.isCorrelated && s.plan.deterministic &&
            isBloomAggregate(s.plan) => f
    }

  private def isBloomAggregate(plan: LogicalPlan): Boolean = plan match {
    case a: Aggregate if a.groupingExpressions.isEmpty =>
      a.aggregateExpressions match {
        case Seq(Alias(expr: AggregateExpression, _)) =>
          expr.aggregateFunction.isInstanceOf[BloomFilterAggregate]
        case _ => false
      }
    case _ => false
  }

  private def sourceFilter(plan: LogicalPlan, key: Attribute): Option[BloomFilterMightContain] =
    plan match {
      case f: Filter => membership(f.condition, key).orElse(sourceFilter(f.child, key))
      case p: Project => childKey(p, key).flatMap(sourceFilter(p.child, _))
      case _ => None
    }

  private def push(
      plan: LogicalPlan,
      key: Attribute,
      filter: BloomFilterMightContain,
      crossedJoin: Boolean): Option[LogicalPlan] = plan match {
    case f: Filter if membership(f.condition, key).isDefined => None
    case f: Filter if f.condition.deterministic =>
      push(f.child, key, filter, crossedJoin).map(p => f.copy(child = p))
    case p: Project if p.projectList.forall(_.deterministic) =>
      childKey(p, key).flatMap(push(p.child, _, filter, crossedJoin)).map(c => p.copy(child = c))
    case j: Join if j.joinType == Inner && j.condition.forall(_.deterministic) =>
      if (j.left.outputSet.contains(key)) {
        push(j.left, key, filter, crossedJoin = true).map(p => j.copy(left = p))
      } else if (j.right.outputSet.contains(key)) {
        push(j.right, key, filter, crossedJoin = true).map(p => j.copy(right = p))
      } else {
        None
      }
    case leaf: LeafNode if crossedJoin && leaf.outputSet.contains(key) =>
      val hash = filter.valueExpression.asInstanceOf[XxHash64].copy(children = Seq(key))
      Some(Filter(filter.copy(valueExpression = hash), leaf))
    case _ => None
  }
}
