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
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.IntegerType

/** Perform one-to-one grouping key restoration after filtering a rank when the window only
 * depends on columns from the preserved side of the lookup.
 */
private[columnar] object RankBeforeLookup {
  val lookupKey = TreeNodeTag[ExprId]("gluten injective grouping lookup key")

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case filter @ Filter(condition, window: Window) if isRankBound(condition, window) =>
      pushRank(filter, window).getOrElse(filter)
  }

  private def isRankBound(condition: Expression, window: Window): Boolean = {
    val rank = window.windowExpressions match {
      case Seq(alias @ Alias(WindowExpression(_: Rank | _: DenseRank, _), _)) =>
        Some(alias.toAttribute)
      case _ => None
    }
    val bound = condition match {
      case LessThanOrEqual(a: Attribute, lit: Literal) if lit.dataType == IntegerType => Some(a)
      case LessThan(a: Attribute, lit: Literal) if lit.dataType == IntegerType => Some(a)
      case _ => None
    }
    rank.exists(r => bound.exists(_.semanticEquals(r))) &&
      window.orderSpec.nonEmpty && window.partitionSpec.nonEmpty &&
      (window.orderSpec ++ window.partitionSpec).forall(_.deterministic)
  }

  private def pushRank(filter: Filter, original: Window): Option[LogicalPlan] = {
    val rank = original.windowExpressions.head.toAttribute
    // WindowGroupLimit is present on Spark 3.5 and later.
    val limit = Some(original.child).filter(p => p.nodeName == "WindowGroupLimit" &&
      p.children.size == 1 && p.references.subsetOf(original.references))

    def visit(
        plan: LogicalPlan,
        window: Window,
        groupLimit: Option[LogicalPlan],
        crossedLookup: Boolean): Option[LogicalPlan] = plan match {
      case project: Project if project.projectList.forall {
            case _: Attribute | Alias(_: Attribute, _) => true
            case _ => false
          } =>
        val source = AttributeMap(project.projectList.collect {
          case a: Attribute => a -> a
          case alias @ Alias(a: Attribute, _) => alias.toAttribute -> a
        })
        def remap(e: Expression): Expression = e.transform {
          case a: Attribute if source.contains(a) => source(a)
        }
        val nextWindow = window.mapExpressions(remap).asInstanceOf[Window]
        visit(project.child, nextWindow, groupLimit.map(_.mapExpressions(remap)), crossedLookup)
          .map(child => Project(project.projectList :+ rank, child))

      case join @ Join(left, _, LeftOuter, Some(EqualTo(_: Attribute, key: Attribute)), _)
          if join.getTagValue(lookupKey).contains(key.exprId) &&
            window.references.subsetOf(left.outputSet) &&
            groupLimit.forall(_.references.subsetOf(left.outputSet)) =>
        visit(left, window, groupLimit, crossedLookup = true).map(child => join.copy(left = child))

      case child if crossedLookup =>
        val input = groupLimit.map(_.withNewChildren(Seq(child))).getOrElse(child)
        Some(Filter(filter.condition, window.copy(child = input)))

      case _ => None
    }

    val input = limit.map(_.children.head).getOrElse(original.child)
    visit(input, original, limit, crossedLookup = false).map(child => Project(filter.output, child))
  }
}
