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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable.ArrayBuffer

/** Compute a SUM argument once per input row when every grouping set uses the same inputs. */
object ProjectExpandMeasures extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.isStreaming || !conf.getConfString("spark.gluten.enabled", "true").toBoolean) {
      return plan
    }
    plan.transformUp {
      case agg: Aggregate => agg.child match {
          case expand: Expand if expand.projections.size > 1 => rewrite(agg, expand)
          case _ => agg
        }
    }
  }

  private def rewrite(agg: Aggregate, expand: Expand): LogicalPlan = {
    val invariant = AttributeMap(expand.output.zipWithIndex.flatMap {
      case (out, i) =>
        expand.projections.head(i) match {
          case source: Attribute if expand.projections.tail.forall(_(i).semanticEquals(source)) =>
            Some(out -> source)
          case _ => None
        }
    })
    if (invariant.isEmpty) return agg

    val measures = ArrayBuffer.empty[Alias]
    def eligible(expr: Expression): Boolean =
      !expr.isInstanceOf[Attribute] && expr.references.nonEmpty && expr.deterministic &&
        expr.references.forall(invariant.contains) && !SubqueryExpression.hasSubquery(expr) &&
        !expr.exists(_.isInstanceOf[UserDefinedExpression])

    val outputs = agg.aggregateExpressions.map(_.transformDown {
      // A filter can protect its argument from an ANSI arithmetic exception.
      case ae @ AggregateExpression(sum: Sum, _, _, None, _) if eligible(sum.child) =>
        val bound = sum.child.transform { case a: Attribute => invariant(a) }
        val alias = measures.find(_.child.semanticEquals(bound)).getOrElse {
          val added = Alias(bound, "_expand_measure")()
          measures += added
          added
        }
        ae.copy(aggregateFunction = sum.copy(child = alias.toAttribute))
    }.asInstanceOf[NamedExpression])

    if (measures.isEmpty) return agg
    val attrs = measures.toList.map(_.toAttribute)
    val projected = Project(expand.child.output ++ measures, expand.child)
    val expanded = expand.copy(
      projections = expand.projections.map(_ ++ attrs),
      output = expand.output ++ attrs,
      child = projected)
    agg.copy(aggregateExpressions = outputs, child = expanded)
  }
}
