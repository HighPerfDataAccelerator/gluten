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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BitOrAgg
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.INTERSECT
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}

/** Combine repeated membership checks on a common dimension into one partial/final aggregation. */
object AggregateIntersectKeys extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(INTERSECT) || !new VeloxConfig(conf).aggregateIntersectKeysEnabled) {
      return plan
    }
    plan.transformDownWithPruning(_.containsPattern(INTERSECT)) {
      case i: Intersect if i.resolved && !i.isAll && !i.isStreaming => rewrite(i)
    }
  }

  private def branches(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Intersect(left, right, false) => branches(left) ++ branches(right)
    case other => Seq(other)
  }

  // All keys must come directly from one joined relation. Compare their ordinals across
  // branches because the analyzer assigns a fresh ExprId to each reference to the relation.
  private def keySource(plan: LogicalPlan): Option[(LogicalPlan, Seq[Int])] = plan match {
    case Project(exprs, child) if child.exists(_.isInstanceOf[Join]) =>
      val keys = exprs.collect {
        case a: Attribute => a
        case Alias(a: Attribute, _) => a
      }
      if (
        keys.size != exprs.size || child.exists(_.expressions.exists(
          e =>
            !e.deterministic || SubqueryExpression.hasSubquery(e) || e.exists(
              _.isInstanceOf[OuterReference])))
      ) {
        return None
      }
      child.collectLeaves().collectFirst {
        case leaf if keys.forall(leaf.outputSet.contains) =>
          (leaf, keys.map(key => leaf.output.indexWhere(_.exprId == key.exprId)))
      }
    case _ => None
  }

  private def rewrite(intersect: Intersect): LogicalPlan = {
    val inputs = branches(intersect)
    val output = intersect.output
    if (
      inputs.size < 3 || inputs.size > 63 || output.isEmpty ||
      !output.forall(
        a =>
          a.dataType match {
            case ByteType | ShortType | IntegerType | LongType => true
            case _ => false
          })
    ) {
      return intersect
    }
    val sources = inputs.flatMap(keySource)
    if (
      sources.size != inputs.size || !sources.tail.forall {
        case (leaf, ordinals) => ordinals == sources.head._2 && leaf.sameResult(sources.head._1)
      } || !inputs.forall(_.output.map(_.nullable) == output.map(_.nullable))
    ) {
      return intersect
    }

    // Duplicate input rows set the same bit. Grouping also gives NULL keys the equality
    // semantics of INTERSECT DISTINCT. The low bits are all set exactly for surviving keys.
    val union = Union(inputs.zipWithIndex.map {
      case (input, index) =>
        Project(input.output :+ Alias(Literal(1L << index), "_intersect_source")(), input)
    })
    val keys = union.output.dropRight(1)
    val mask = Alias(BitOrAgg(union.output.last).toAggregateExpression(), "_intersect_mask")()
    val grouped = Aggregate(keys, keys :+ mask, union)
    Project(keys, Filter(EqualTo(mask.toAttribute, Literal((1L << inputs.size) - 1L)), grouped))
  }
}
