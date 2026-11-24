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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types._

/**
 * Shares the build-side scan of a semi join followed by an anti join when both look for a different
 * integral value at the same key and the anti join searches a filtered subset. A non-null probe
 * differs from some member of a set exactly when it differs from its minimum or maximum. An empty
 * or all-null subset contributes no anti match.
 */
object ReduceExistenceJoins extends Rule[LogicalPlan] with PredicateHelper {
  private case class Keys(
      probe: Attribute,
      build: Attribute,
      probeValue: Attribute,
      buildValue: Attribute)
  private case class Scan(leaf: LogicalRelation, filters: Seq[Expression])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.isStreaming || !conf.getConfString("spark.gluten.enabled", "true").toBoolean)
      return plan
    plan.transformUp {
      case anti @ Join(
            semi @ Join(_, _, LeftSemi, _, JoinHint.NONE),
            _,
            LeftAnti,
            _,
            JoinHint.NONE) =>
        rewrite(semi, anti).getOrElse(anti)
    }
  }

  private def keys(join: Join): Option[Keys] = {
    def oriented(a: Attribute, b: Attribute): Option[(Attribute, Attribute)] =
      if (join.left.outputSet.contains(a) && join.right.outputSet.contains(b)) Some(a -> b)
      else if (join.left.outputSet.contains(b) && join.right.outputSet.contains(a)) Some(b -> a)
      else None
    val parts = join.condition.toSeq.flatMap(splitConjunctivePredicates)
    if (parts.size != 2) return None
    val equality =
      parts.collectFirst { case EqualTo(a: Attribute, b: Attribute) => oriented(a, b) }.flatten
    val inequality =
      parts.collectFirst { case Not(EqualTo(a: Attribute, b: Attribute)) => oriented(a, b) }.flatten
    for {
      (probe, build) <- equality
      (probeValue, buildValue) <- inequality
      if Seq(ByteType, ShortType, IntegerType, LongType).contains(buildValue.dataType)
    } yield Keys(probe, build, probeValue, buildValue)
  }

  private def scan(plan: LogicalPlan): Option[Scan] = plan match {
    case p: Project if p.projectList.forall(_.isInstanceOf[Attribute]) => scan(p.child)
    case Filter(c, child) =>
      scan(child).map(s => s.copy(filters = s.filters ++ splitConjunctivePredicates(c)))
    case l: LogicalRelation => l.relation match {
        case h: HadoopFsRelation if h.fileFormat.isInstanceOf[ParquetFileFormat] =>
          Some(Scan(l, Nil))
        case _ => None
      }
    case _ => None
  }

  private def simplePredicate(e: Expression): Boolean = e match {
    case c: BinaryComparison =>
      c.children.forall(x => x.isInstanceOf[Attribute] || x.isInstanceOf[Literal])
    case IsNotNull(_: Attribute) | IsNull(_: Attribute) => true
    case a: Attribute => a.dataType == BooleanType
    case _ => false
  }

  private def rewrite(semi: Join, anti: Join): Option[LogicalPlan] = for {
    s <- keys(semi)
    a <- keys(anti) if s.probe.semanticEquals(a.probe) && s.probeValue.semanticEquals(a.probeValue)
    all <- scan(semi.right)
    subset <- scan(anti.right) if all.leaf.sameResult(subset.leaf)
    if all.filters.forall {
      case IsNotNull(x: Attribute) => x.semanticEquals(s.build) || x.semanticEquals(s.buildValue)
      case _ => false
    }
    if subset.filters.nonEmpty && subset.filters.forall(simplePredicate)
    mapping = AttributeMap(subset.leaf.output.zip(all.leaf.output))
    if mapping.get(a.build).exists(_.semanticEquals(s.build)) && mapping.get(
      a.buildValue).exists(_.semanticEquals(s.buildValue))
  } yield {
    val filter = subset.filters.reduceLeft(And).transform { case x: Attribute => mapping(x) }
    // Filtering the aggregate input would also remove rows used by the semi join.
    val limited = Alias(
      If(filter, s.buildValue, Literal.create(null, s.buildValue.dataType)),
      "_matching_value")()
    val projected = Project(Seq(s.build, s.buildValue).distinct :+ limited, all.leaf)
    val low = Alias(Min(s.buildValue).toAggregateExpression(), "_low")()
    val high = Alias(Max(s.buildValue).toAggregateExpression(), "_high")()
    val lowMatch = Alias(Min(limited.toAttribute).toAggregateExpression(), "_matching_low")()
    val highMatch = Alias(Max(limited.toAttribute).toAggregateExpression(), "_matching_high")()
    val bounds = Aggregate(Seq(s.build), Seq(s.build, low, high, lowMatch, highMatch), projected)
    val different =
      Or(Not(EqualTo(s.probeValue, low.toAttribute)), Not(EqualTo(s.probeValue, high.toAttribute)))
    val noDifferent = Or(
      IsNull(lowMatch.toAttribute),
      And(
        EqualTo(s.probeValue, lowMatch.toAttribute),
        EqualTo(s.probeValue, highMatch.toAttribute)))
    val condition = And(EqualTo(s.probe, s.build), And(different, noDifferent))
    Project(semi.left.output, Join(semi.left, bounds, Inner, Some(condition), JoinHint.NONE))
  }
}
