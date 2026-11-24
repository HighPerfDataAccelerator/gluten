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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.{CollapseProject, ColumnPruning, RemoveNoopOperators}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

object DimensionStringGroupingIds {
  val key = "spark.gluten.sql.columnar.backend.velox.aggregation.dimensionStringIds.enabled"
  // Run late, after Spark's own aggregate and projection rewrites.
  def install(session: SparkSession): Rule[LogicalPlan] = {
    val methods = session.experimental
    methods.synchronized {
      methods.extraOptimizations = methods.extraOptimizations
        .filterNot(_.isInstanceOf[DimensionStringGroupingIds]) :+
        DimensionStringGroupingIds(session)
    }
    new Rule[LogicalPlan] { override def apply(plan: LogicalPlan): LogicalPlan = plan }
  }
  val keyName = "gluten_grouping_key"
  private val buckets = 64
  private val rankBits = 24
  val idName = "gluten_grouping_id"
}

/**
 * Large aggregations, especially grouping sets that multiply every input row, often group by string
 * columns of a small dimension table. Hashing, expanding, spilling and shuffling those strings
 * dominates the work. Replace each such grouping key by an injective integer computed on the
 * dimension side (dense rank of the key value; NULL gets its own rank) and restore the string after
 * aggregation through a small left outer join on the integer. Grouping on an injective image forms
 * exactly the same groups, and NULL identifiers produced by grouping sets do not match any restored
 * value, so results are unchanged. Default disabled.
 */
case class DimensionStringGroupingIds(session: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper with Logging {
  import DimensionStringGroupingIds._

  private def simpleScan(plan: LogicalPlan): Boolean = plan match {
    case Project(list, child) => list.forall(_.deterministic) && simpleScan(child)
    case Filter(condition, child) =>
      condition.deterministic && !SubqueryExpression.hasSubquery(condition) && simpleScan(child)
    case leaf: LeafNode => leaf.isInstanceOf[MultiInstanceRelation] && !leaf.isStreaming
    case _ => false
  }

  private def scanBytes(plan: LogicalPlan): BigInt = {
    val unknown = session.sessionState.conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    plan.collect {
      case leaf: LeafNode =>
        val bytes = leaf.stats.sizeInBytes
        if (bytes == unknown) BigInt(-1) else bytes
    }.foldLeft(BigInt(0))(_ max _)
  }

  // Scanned bytes on paths that contribute rows to the aggregation: the
  // existence side of a semi/anti join only filters and is not counted.
  private def rowScanBytes(plan: LogicalPlan): BigInt = plan match {
    case Join(left, _, LeftSemi | LeftAnti | ExistenceJoin(_), _, _) => rowScanBytes(left)
    case leaf: LeafNode => scanBytes(leaf)
    case other => other.children.map(rowScanBytes).foldLeft(BigInt(0))(_ max _)
  }

  /** Resolve an attribute of `plan` to an expression over the output of a small dimension. */
  private def resolve(
      attr: Attribute,
      plan: LogicalPlan,
      dimensionMax: Long): Option[(LogicalPlan, Expression)] = {
    if (!plan.outputSet.contains(attr)) return None
    if (simpleScan(plan)) {
      val bytes = scanBytes(plan)
      return if (bytes >= 0 && bytes <= dimensionMax) Some((plan, attr)) else None
    }
    plan match {
      case Project(list, child) =>
        list.find(_.exprId == attr.exprId) match {
          case Some(a: Attribute) => resolve(a, child, dimensionMax)
          case Some(Alias(e, _)) if e.deterministic && e.references.nonEmpty =>
            val parts = e.references.toSeq.map(r => r -> resolve(r, child, dimensionMax))
            val dims = parts.flatMap(_._2.map(_._1)).distinct
            if (
              parts.exists(_._2.isEmpty) || dims.size != 1 || !dims.forall(
                d => parts.forall(p => p._2.get._1 eq d))
            ) {
              None
            } else {
              val m = parts.map { case (r, v) => r.exprId -> v.get._2 }.toMap
              Some((
                dims.head,
                e.transform {
                  case a: Attribute if m.contains(a.exprId) => m(a.exprId)
                }))
            }
          case _ => None
        }
      case Filter(_, child) => resolve(attr, child, dimensionMax)
      case Join(left, right, Inner, _, _) =>
        if (left.outputSet.contains(attr)) resolve(attr, left, dimensionMax)
        else resolve(attr, right, dimensionMax)
      case _ => None
    }
  }

  /** A copy of a simple dimension scan with fresh expression ids. */
  private def freshCopy(plan: LogicalPlan): Option[(LogicalPlan, Map[ExprId, Attribute])] =
    plan match {
      case leaf: LeafNode with MultiInstanceRelation =>
        val copy = leaf.newInstance()
        Some((copy, leaf.output.map(_.exprId).zip(copy.output).toMap))
      case Filter(condition, child) =>
        freshCopy(child).map { case (c, m) => (Filter(remap(condition, m), c), m) }
      case Project(list, child) =>
        freshCopy(child).map {
          case (c, m) =>
            val fresh = list.map {
              case a: Attribute => m(a.exprId)
              case Alias(e, name) => Alias(remap(e, m), name)()
              case other => other
            }
            (Project(fresh, c), list.map(_.exprId).zip(fresh.map(_.toAttribute)).toMap)
        }
      case _ => None
    }

  private def remap(e: Expression, m: Map[ExprId, Attribute]): Expression = e.transform {
    case a: Attribute if m.contains(a.exprId) => m(a.exprId)
  }

  /**
   * Adds an injective identifier for `key`: the hash bucket in the high bits and the dense rank of
   * the key within its bucket in the low 24 bits. Buckets rank in parallel; NULL keys get their own
   * rank. Returns the plan, the key attribute and the identifier attribute.
   */
  private def identify(plan: LogicalPlan, key: Expression): (LogicalPlan, Attribute, Attribute) = {
    val k = Alias(key, keyName)()
    val bucket = Alias(Pmod(new Murmur3Hash(Seq(key)), Literal(buckets)), keyName + "_bucket")()
    val spec = WindowSpecDefinition(
      Seq(bucket.toAttribute),
      Seq(SortOrder(k.toAttribute, Ascending)),
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))
    val rank = Alias(WindowExpression(DenseRank(Seq(k.toAttribute)), spec), idName + "_rank")()
    val window = Window(
      Seq(rank),
      spec.partitionSpec,
      spec.orderSpec,
      Project(plan.output :+ k :+ bucket, plan))
    // 64 buckets x 2^24 ranks fit a positive 32-bit integer. A dimension
    // small enough to be encoded never reaches the limit; if it ever did,
    // fail the query instead of merging groups.
    val value = Add(Multiply(bucket.toAttribute, Literal(1 << rankBits)), rank.toAttribute)
    val id = Alias(
      If(
        LessThan(rank.toAttribute, Literal(1 << rankBits)),
        value,
        Cast(new RaiseError(Literal("Dimension string identifier overflow")), IntegerType)),
      idName)()
    (Project(plan.output :+ k.toAttribute :+ id, window), k.toAttribute, id.toAttribute)
  }

  /** Dimension rows plus one identifier per key expression, in order. */
  private def encode(
      dimension: LogicalPlan,
      keys: Seq[Expression]): (LogicalPlan, Seq[Attribute]) = {
    keys.foldLeft((dimension, Seq.empty[Attribute])) {
      case ((plan, ids), key) =>
        val (encoded, _, id) = identify(plan, key)
        (Project(plan.output :+ id, encoded), ids :+ id)
    }
  }

  /** Distinct (id, key) restoring table built from an independent copy of the dimension. */
  private def mapping(
      dimension: LogicalPlan,
      key: Expression): Option[(LogicalPlan, Attribute, Attribute)] = {
    freshCopy(dimension).map {
      case (copy, m) =>
        val (encoded, k, id) = identify(copy, remap(key, m))
        val keys = Seq(k, id)
        (Aggregate(keys, keys, encoded), id, k)
    }
  }

  private case class Candidate(output: Attribute, dimension: LogicalPlan, key: Expression)

  private def rewrite(agg: Aggregate): Option[LogicalPlan] = {
    val veloxConf = new VeloxConfig(session.sessionState.conf)
    val dimensionMax = veloxConf.dimensionStringIdsDimensionMaxBytes
    if (rowScanBytes(agg.child) < veloxConf.dimensionStringIdsFactMinBytes) return None
    // Grouping keys only appear as bare pass-through outputs; any other use
    // (inside aggregate functions or expressions) keeps the original key.
    def passThroughOnly(a: Attribute): Boolean = agg.aggregateExpressions.forall {
      case b: Attribute => true
      case Alias(b: Attribute, _) => true
      case other => !other.references.contains(a)
    }
    agg.child match {
      case expand @ Expand(projections, output, child) =>
        val found = output.indices.flatMap {
          i =>
            val o = output(i)
            val sources = projections.map(_(i)).filterNot {
              case Literal(null, _) => true
              case _ => false
            }.distinct
            if (
              o.dataType != StringType || !agg.groupingExpressions.exists(_.semanticEquals(o)) ||
              !passThroughOnly(o) || sources.size != 1 || !sources.head.isInstanceOf[Attribute]
            ) {
              None
            } else {
              resolve(sources.head.asInstanceOf[Attribute], child, dimensionMax)
                .map { case (d, k) => (i, Candidate(o, d, k)) }
            }
        }
        if (found.isEmpty) return None
        val (newChild, ids) = attach(child, found.map(_._2))
        val newOutput = output.indices.map {
          i =>
            found.find(_._1 == i).map(
              f =>
                AttributeReference(
                  f._2.output.name + "_id",
                  IntegerType)().asInstanceOf[Attribute]).getOrElse(output(i))
        }
        val newProjections = projections.map {
          p =>
            p.indices.map {
              i =>
                found.indexWhere(_._1 == i) match {
                  case -1 => p(i)
                  case j => p(i) match {
                      case Literal(null, _) => Literal(null, IntegerType)
                      case _ => ids(j)
                    }
                }
            }
        }
        val replaced = found.map { case (i, c) => c.output -> newOutput(i) }
        Some(finish(agg, Expand(newProjections, newOutput, newChild), replaced, found.map(_._2)))
      case child =>
        val found = agg.groupingExpressions.flatMap {
          // Without grouping sets, plain dictionary columns are cheap enough
          // that extra identifier stages do not pay off. Only encode keys the
          // aggregate drops and that are computed from dimension columns: the
          // expression is then evaluated once per dimension row instead of once
          // per joined fact row, and no restoring join is needed.
          case a: Attribute
              if a.dataType == StringType &&
                !agg.aggregateExpressions.exists(_.references.contains(a)) =>
            resolve(a, child, dimensionMax).collect {
              case (d, k) if !k.isInstanceOf[Attribute] => Candidate(a, d, k)
            }
          case _ => None
        }
        if (found.isEmpty) return None
        val (newChild, ids) = attach(child, found)
        Some(finish(agg, newChild, found.map(_.output).zip(ids), found))
    }
  }

  /** Replace each dimension by its encoded form and carry identifiers upward. */
  private def attach(child: LogicalPlan, found: Seq[Candidate]): (LogicalPlan, Seq[Attribute]) = {
    val dimensions = found.map(_.dimension).foldLeft(Seq.empty[LogicalPlan]) {
      (seen, d) => if (seen.exists(_ eq d)) seen else seen :+ d
    }
    val encoded = dimensions.map(d => d -> encode(d, found.filter(_.dimension eq d).map(_.key)))
    val ids = found.map {
      c =>
        val (_, (_, dimensionIds)) = encoded.find(_._1 eq c.dimension).get
        dimensionIds(found.filter(_.dimension eq c.dimension).indexOf(c))
    }
    // Bottom-up: the encoded form contains the original dimension, which must
    // not be visited again.
    val replaced = child.transformUp {
      case p if encoded.exists(_._1 eq p) => encoded.find(_._1 eq p).get._2._1
    }
    val carried = replaced.transformUp {
      case p @ Project(list, c) =>
        val missing = ids.filter(id => c.outputSet.contains(id) && !p.outputSet.contains(id))
        if (missing.isEmpty) p else Project(list ++ missing, c)
    }
    (carried, ids)
  }

  private def finish(
      agg: Aggregate,
      child: LogicalPlan,
      replaced: Seq[(Attribute, Attribute)],
      found: Seq[Candidate]): LogicalPlan = {
    val swap = replaced.map { case (o, n) => o.exprId -> n }.toMap
    def sub(e: Expression): Expression = e.transform {
      case a: Attribute if swap.contains(a.exprId) => swap(a.exprId)
    }
    val grouping = agg.groupingExpressions.map(sub)
    val outputs = agg.aggregateExpressions.map {
      case a: Attribute if swap.contains(a.exprId) => swap(a.exprId)
      case al @ Alias(a: Attribute, name) if swap.contains(a.exprId) =>
        Alias(swap(a.exprId), name)(exprId = al.exprId)
      case other => other
    }
    val newAgg = Aggregate(grouping, outputs, child)
    // Restore strings only for keys the aggregate actually returns.
    val restored = agg.aggregateExpressions.zip(outputs).flatMap {
      case (orig, now)
          if orig.toAttribute.dataType == StringType &&
            now.toAttribute.dataType == IntegerType =>
        val source = orig match {
          case a: Attribute => a
          case Alias(a: Attribute, _) => a
        }
        found.zip(replaced).find(_._2._1.exprId == source.exprId).map {
          case (c, _) => (orig.toAttribute, now.toAttribute, c)
        }
      case _ => None
    }
    if (restored.isEmpty) {
      logInfo(s"DimensionStringGroupingIds encoded ${found.size} grouping keys")
      return newAgg
    }
    val (joined, values) = restored.foldLeft((newAgg: LogicalPlan, Map.empty[ExprId, Attribute])) {
      case ((plan, m), (orig, id, c)) =>
        mapping(c.dimension, c.key) match {
          case Some((table, mid, mkey)) =>
            val lookup = Join(plan, table, LeftOuter, Some(EqualTo(id, mid)), JoinHint.NONE)
            lookup.setTagValue(RankBeforeLookup.lookupKey, mid.exprId)
            (lookup, m + (orig.exprId -> mkey))
          case None => return newAgg
        }
    }
    logInfo(s"DimensionStringGroupingIds encoded ${found.size} grouping keys, restored " +
      s"${restored.size}")
    Project(
      agg.output.map {
        o => values.get(o.exprId).map(k => Alias(k, o.name)(exprId = o.exprId)).getOrElse(o)
      },
      joined)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    if (
      !new VeloxConfig(conf).dimensionStringIdsEnabled ||
      !conf.getConfString("spark.gluten.enabled", "true").toBoolean || plan.isStreaming
    ) return plan
    var changed = false
    val result = plan.transformUp {
      case agg: Aggregate if !agg.groupingExpressions.exists(_.isInstanceOf[AggregateExpression]) =>
        rewrite(agg).map {
          r =>
            changed = true
            r
        }.getOrElse(agg)
    }
    if (changed) {
      val cleaned = RemoveNoopOperators(CollapseProject(ColumnPruning(result)))
      RankBeforeLookup(cleaned)
    } else plan
  }
}
