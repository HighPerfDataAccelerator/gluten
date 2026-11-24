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
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.internal.SQLConf

/** Join an unfiltered dimension to the smaller input of a compound inner join. */
case class ReorderFilteredCompoundJoin(session: SparkSession)
  extends SparkStrategy with PredicateHelper {
  private def unwrap(plan: LogicalPlan): LogicalPlan = plan match {
    case Project(cols, child) if cols.forall(_.isInstanceOf[Attribute]) => unwrap(child)
    case p => p
  }

  private def scan(plan: LogicalPlan, nullChecksOnly: Boolean): Boolean = unwrap(plan) match {
    case Filter(cond, c) =>
      (!nullChecksOnly || splitConjunctivePredicates(cond).forall {
        case IsNotNull(_: Attribute) => true
        case _ => false
      }) && scan(c, nullChecksOnly)
    case _: LeafNode => true
    case _ => false
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

  // A shared selective filter on the other join key makes scan sizes comparable.
  private def filters(plan: LogicalPlan, key: Attribute): Seq[(LogicalPlan, Long)] =
    plan.collect {
      case Filter(cond, _) => splitConjunctivePredicates(cond).collect {
          case BloomFilterMightContain(s: ScalarSubquery, XxHash64(Seq(k: Attribute), seed))
              if k.semanticEquals(key) && !s.isCorrelated => (s.plan, seed)
        }
    }.flatten

  private def commonFilter(
      left: LogicalPlan,
      right: LogicalPlan,
      a: Attribute,
      b: Attribute): Boolean = {
    val candidates = filters(right, b)
    filters(left, a).exists {
      case (first, seed) =>
        candidates.exists { case (second, other) => seed == other && first.sameResult(second) }
    }
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

  private def reorder(outer: Join): Option[LogicalPlan] = for {
    inner <- unwrap(outer.left) match {
      case j @ Join(_, _, Inner, Some(_), JoinHint.NONE) => Some(j)
      case _ => None
    }
    if unwrap(inner.left).isInstanceOf[Join] && scan(inner.right, nullChecksOnly = true) &&
      scan(outer.right, nullChecksOnly = false)
    dimension <- pair(inner.condition.get, inner.left, inner.right)
    conjuncts = splitConjunctivePredicates(outer.condition.get)
    if conjuncts.size == 2
    pairs = conjuncts.flatMap(pair(_, inner.left, outer.right))
    if pairs.size == 2 && pairs.map(_._1).distinct.size == 2 && pairs.map(_._2).distinct.size == 2
    bridgeKey <- pairs.find(_._1.semanticEquals(dimension._1)).map(_._2)
    other <- pairs.find(p => !p._1.semanticEquals(dimension._1))
    if commonFilter(inner.left, outer.right, other._1, other._2)
    fact <- knownScanSize(inner.left)
    bridge <- knownScanSize(outer.right)
    dim <- knownScanSize(inner.right)
    conf = session.sessionState.conf
    broadcast = math.max(
      conf.autoBroadcastJoinThreshold,
      conf.getConf(
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD).getOrElse(conf.autoBroadcastJoinThreshold))
    if fact >= 4 * bridge && bridge >= 4 * dim && inner.right.stats.sizeInBytes > broadcast
  } yield {
    // The outer equality supplies the missing bridge-to-dimension equality. Keep both original
    // outer equalities, including the other component of the compound key.
    val attached =
      Join(outer.right, inner.right, Inner, Some(EqualTo(bridgeKey, dimension._2)), JoinHint.NONE)
    Project(outer.output, Join(inner.left, attached, Inner, outer.condition, JoinHint.NONE))
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case j @ Join(_, _, Inner, Some(_), JoinHint.NONE)
        if !j.isStreaming && new VeloxConfig(
          session.sessionState.conf).reorderFilteredCompoundJoinEnabled =>
      reorder(j).map(p => planLater(p)).toSeq
    case _ => Nil
  }
}
