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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.LEFT_SEMI_OR_ANTI_JOIN
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Shares the build-side scan of a semi join followed by an anti join when both look for a different
 * integral value at the same key and the anti join searches a filtered subset. A non-null probe
 * differs from some member of a set exactly when it differs from its minimum or maximum. An empty
 * or all-null subset contributes no anti match.
 */
object ReduceExistenceJoins extends Rule[LogicalPlan] with PredicateHelper {
  object FoldExistenceCounts extends SparkStrategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case a: Aggregate
          if a.containsPattern(LEFT_SEMI_OR_ANTI_JOIN) && !a.isStreaming &&
            conf.getConfString("spark.gluten.enabled", "true").toBoolean =>
        weighted(a).map(planLater).toSeq
      case _ => Nil
    }
  }

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

  private def matchingScans(semi: Join, anti: Join): Option[(Keys, Scan, Seq[Expression])] = for {
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
    if subset.filters.forall(_.references.subsetOf(subset.leaf.outputSet))
    if mapping.get(a.build).exists(_.semanticEquals(s.build)) && mapping.get(
      a.buildValue).exists(_.semanticEquals(s.buildValue))
  } yield (s, all, subset.filters.map(_.transform { case x: Attribute => mapping(x) }))

  private def rewrite(semi: Join, anti: Join): Option[LogicalPlan] =
    matchingScans(semi, anti).map {
      case (s, all, conditions) =>

        val filter = conditions.reduceLeft(And)
        // Filtering the aggregate input would also remove rows used by the semi join.
        val limited = Alias(
          If(filter, s.buildValue, Literal.create(null, s.buildValue.dataType)),
          "_matching_value")()
        val projected = Project(Seq(s.build, s.buildValue).distinct :+ limited, all.leaf)
        val low = Alias(Min(s.buildValue).toAggregateExpression(), "_low")()
        val high = Alias(Max(s.buildValue).toAggregateExpression(), "_high")()
        val lowMatch = Alias(Min(limited.toAttribute).toAggregateExpression(), "_matching_low")()
        val highMatch = Alias(Max(limited.toAttribute).toAggregateExpression(), "_matching_high")()
        val bounds =
          Aggregate(Seq(s.build), Seq(s.build, low, high, lowMatch, highMatch), projected)
        val different =
          Or(
            Not(EqualTo(s.probeValue, low.toAttribute)),
            Not(EqualTo(s.probeValue, high.toAttribute)))
        val noDifferent = Or(
          IsNull(lowMatch.toAttribute),
          And(
            EqualTo(s.probeValue, lowMatch.toAttribute),
            EqualTo(s.probeValue, highMatch.toAttribute)))
        val condition = And(EqualTo(s.probe, s.build), And(different, noDifferent))
        Project(semi.left.output, Join(semi.left, bounds, Inner, Some(condition), JoinHint.NONE))
    }

  private type WeightedPlan = (LogicalPlan, Attribute)

  // A qualifying probe comes from the filtered subset. If a key has multiple non-null
  // values overall and exactly one in that subset, every matching probe has the same value.
  // Count these probes before the outer joins and carry their multiplicity to COUNT(*).
  private def filteredCount(semi: Join, anti: Join): Option[WeightedPlan] = for {
    (s, all, preds) <- matchingScans(semi, anti)
    if semi.left.outputSet == AttributeSet(Seq(
      s.probe,
      s.probeValue)) && s.probe.exprId != s.probeValue.exprId
    probe <- scan(semi.left) if probe.leaf.sameResult(all.leaf)
    pr = AttributeMap(probe.leaf.output.zip(all.leaf.output))
    if pr.get(s.probe).exists(_.semanticEquals(s.build)) && pr.get(
      s.probeValue).exists(_.semanticEquals(s.buildValue))
    if probe.filters.forall(
      x =>
        x.references.subsetOf(probe.leaf.outputSet) && !x.exists {
          case s: SubqueryExpression => s.isCorrelated
          case _ => false
        })
    translate = (e: Expression) => e.transform { case x: Attribute => pr(x) }
    if preds.forall(e => probe.filters.exists(x => translate(x).semanticEquals(e)))
    extra = probe.filters.filterNot(x => preds.exists(_.semanticEquals(translate(x))))
    if extra.forall(e => e.deterministic && e.references.subsetOf(semi.left.outputSet))
  } yield {
    // Explicit null checks on comparison operands do not change which values the aggregate sees.
    val required = AttributeSet(preds.collect {
      case c: BinaryComparison if !c.isInstanceOf[EqualNullSafe] => c
    }.flatMap(_.references))
    val needed = preds.filterNot {
      case IsNotNull(k: Attribute) => required.contains(k)
      case _ => false
    }
    def runtimeFilterOn(expression: Expression, key: Attribute): Boolean = expression match {
      case BloomFilterMightContain(sub: ScalarSubquery, XxHash64(Seq(k: Attribute), _))
          if k.semanticEquals(key) && !sub.isCorrelated && sub.plan.deterministic =>
        sub.plan match {
          case agg: Aggregate if agg.groupingExpressions.isEmpty =>
            agg.aggregateExpressions match {
              case Seq(Alias(e: AggregateExpression, _)) =>
                e.aggregateFunction.isInstanceOf[BloomFilterAggregate]
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
    val (early, remaining) = extra.partition {
      case IsNotNull(k: Attribute) => k.semanticEquals(s.probe)
      case e => runtimeFilterOn(e, s.probe)
    }
    val keyFilters = (Seq(IsNotNull(s.build)) ++ early.map(translate)).distinct
    val kept = Filter(keyFilters.reduceLeft(And), all.leaf)
    val predicate = needed.reduceLeft(And)
    // A value filter can eliminate groups but cannot be pushed onto their individual members.
    // Keep every member of groups reached by the probe, including members with different values.
    val (input, qualifies) = if (remaining.exists(runtimeFilterOn(_, s.probeValue))) {
      val candidateKey = Alias(s.probe, "_candidate_group_key")()
      val keys = Project(Seq(candidateKey), semi.left)
      val flag = Alias(predicate, "_member_of_subset")()
      val members = Project(Seq(s.build, s.buildValue, flag), kept)
      val selected = Join(
        members,
        keys,
        LeftSemi,
        Some(EqualTo(s.build, candidateKey.toAttribute)),
        JoinHint.NONE)
      (selected, flag.toAttribute)
    } else {
      (kept, predicate)
    }
    val matched = Alias(
      If(qualifies, s.buildValue, Literal.create(null, s.buildValue.dataType)),
      "_qualifying_value")()
    val projected = Project(Seq(s.build, s.buildValue, matched), input)
    val min = Alias(Min(s.buildValue).toAggregateExpression(), "_min")()
    val max = Alias(Max(s.buildValue).toAggregateExpression(), "_max")()
    val low = Alias(Min(matched.toAttribute).toAggregateExpression(), "_matching_low")()
    val high = Alias(Max(matched.toAttribute).toAggregateExpression(), "_matching_high")()
    val count =
      Alias(Count(Seq(matched.toAttribute)).toAggregateExpression(), "_qualifying_count")()
    val grouped = Aggregate(Seq(s.build), Seq(s.build, min, max, low, high, count), projected)
    val accepted = Filter(
      And(LessThan(min.toAttribute, max.toAttribute), EqualTo(low.toAttribute, high.toAttribute)),
      grouped)
    val fields = semi.left.output.map {
      orig =>
        val v = if (orig.semanticEquals(s.probe)) s.build else low.toAttribute
        Alias(v, orig.name)(
          exprId = orig.exprId,
          qualifier = orig.qualifier,
          explicitMetadata = Some(orig.metadata))
    }
    val narrow = Project(fields :+ count.toAttribute, accepted)
    (remaining.reduceLeftOption(And).map(Filter(_, narrow)).getOrElse(narrow), count.toAttribute)
  }

  private def search(plan: LogicalPlan): Option[WeightedPlan] = plan match {
    case a @ Join(s @ Join(_, _, LeftSemi, _, JoinHint.NONE), _, LeftAnti, _, JoinHint.NONE) =>
      filteredCount(s, a)
    case f: Filter if f.condition.deterministic =>
      search(f.child).map { case (d, k) => (f.copy(child = d), k) }
    case p: Project if p.projectList.forall(_.deterministic) =>
      search(p.child).map {
        case (d, k) => (p.copy(projectList = p.projectList :+ k, child = d), k)
      }
    case j @ Join(l, r, Inner, c, _) if c.forall(_.deterministic) =>
      search(l).map { case (d, k) => (j.copy(left = d), k) }.orElse(search(r).map {
        case (d, k) => (j.copy(right = d), k)
      })
    case _ => None
  }

  private def weighted(a: Aggregate): Option[LogicalPlan] = {
    if (a.groupingExpressions.isEmpty || !a.groupingExpressions.forall(_.deterministic)) return None
    val counts = a.aggregateExpressions.flatMap(_.collect { case e: AggregateExpression => e })
    if (counts.size != 1 || !a.aggregateExpressions.forall(_.deterministic)) return None
    val e = counts.head
    val literal = e.aggregateFunction match {
      case Count(Seq(l: Literal)) => l.value != null
      case _ => false
    }
    if (!literal || e.isDistinct || e.filter.isDefined) return None
    search(a.child).map {
      case (child, weight) =>
        // COUNT accumulates a Long with wrapping arithmetic even in ANSI mode.
        val countConf = new SQLConf
        countConf.setConf(SQLConf.ANSI_ENABLED, false)
        val sum = SQLConf.withExistingConf(countConf)(Sum(weight).toAggregateExpression())
        val total = Coalesce(Seq(sum, Literal(0L)))
        val outputs = a.aggregateExpressions.map(_.transform {
          case old: AggregateExpression if old == e => total
        }.asInstanceOf[NamedExpression])
        a.copy(aggregateExpressions = outputs, child = child)
    }
  }
}
