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
package org.apache.gluten.experimental.q5

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, ConstantFolding, PushDownPredicates}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

/** Register after GlutenSessionExtensions so this rule follows the first cascade wave. */
class Q5SecondHopBloomExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(Q5SecondHopBloom.install)
  }
}

object Q5SecondHopBloom {
  val enabledKey = VeloxConfig.Q5_SECOND_HOP_BLOOM_ENABLED.key
  val marker = "gluten_q5_second_hop_bloom_filter"
  val firstMarker = "gluten_cascading_bloom_filter"
  val injected = TreeNodeTag[Boolean]("gluten.q5SecondHopBloom.injected")

  def install(session: SparkSession): Rule[LogicalPlan] = {
    val methods = session.experimental
    methods.synchronized {
      methods.extraOptimizations = methods.extraOptimizations
        .filterNot(_.isInstanceOf[Q5SecondHopBloom]) :+ Q5SecondHopBloom(session)
    }
    new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan
    }
  }
}

case class Q5SecondHopBloom(session: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper with Logging {
  import Q5SecondHopBloom._

  private def markedFilters(plan: LogicalPlan, wanted: String): Int = plan.collect {
    case Filter(condition, _) => condition.collect {
        case BloomFilterMightContain(query: ScalarSubquery, _) if query.plan.exists {
              case aggregate: Aggregate => aggregate.aggregateExpressions.exists(_.name == wanted)
              case _ => false
            } => 1
      }.sum
  }.sum

  private def simpleScan(plan: LogicalPlan): Boolean = plan match {
    // Attribute-only projection keeps the exact key identity through the subtree.
    case Project(expressions, child) =>
      expressions.forall(_.isInstanceOf[Attribute]) && simpleScan(child)
    case Filter(condition, child) => condition.deterministic && !condition.exists {
        case _: ScalaUDF | _: PythonUDF | _: OuterReference => true
        case query: SubqueryExpression => query.isCorrelated
        case _ => false
      } && simpleScan(child)
    case leaf: LeafNode => !leaf.isStreaming && leaf.deterministic
    case _ => false
  }

  private def hasBloomForKey(plan: LogicalPlan, wanted: Expression): Boolean = plan.exists {
    case Filter(condition, _) => splitConjunctivePredicates(condition).exists {
        case BloomFilterMightContain(_, XxHash64(Seq(value), _)) => value.semanticEquals(wanted)
        case _ => false
      }
    case _ => false
  }

  private def maxScanBytes(plan: LogicalPlan): BigInt = {
    val unknown = session.sessionState.conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    plan.collect {
      case leaf: LeafNode =>
        val bytes = leaf.stats.sizeInBytes
        if (bytes == unknown) BigInt(0) else bytes
    }.foldLeft(BigInt(0))(_ max _)
  }

  private def target(join: Join): Option[(Attribute, Attribute)] = join match {
    case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, otherCondition, _, left, right, _)
        if otherCondition.isEmpty && leftKeys.size == 2 && rightKeys.size == 2 &&
          !join.getTagValue(injected).contains(true) && simpleScan(right) &&
          right.output.map(_.name).toSet == Set("s_suppkey", "s_nationkey") &&
          markedFilters(right, firstMarker) == 1 =>
      val pairs = leftKeys.zip(rightKeys)
      val supplier = pairs.collectFirst {
        case (l: Attribute, r: Attribute)
            if l.name == "l_suppkey" && r.name == "s_suppkey" &&
              l.dataType == LongType && r.dataType == LongType => (l, r)
      }
      val nation = pairs.exists {
        case (l: Attribute, r: Attribute) =>
          l.name == "c_nationkey" && r.name == "s_nationkey" &&
          l.dataType == LongType && r.dataType == LongType
        case _ => false
      }
      if (nation) supplier else None
    case _ => None
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = session.sessionState.conf
    val veloxConf = new VeloxConfig(conf)
    if (
      !conf.getConfString(enabledKey, "false").toBoolean ||
      !conf.getConfString("spark.gluten.enabled", "true").toBoolean ||
      !veloxConf.runtimeBloomCascadeEnabled || plan.isStreaming
    ) return plan
    val maximum = veloxConf.runtimeBloomCascadeMaxFilters
    val maxKeyBytes = veloxConf.runtimeBloomCascadeMaxCreationKeyBytes
    require(maximum >= 0 && maximum <= 10 && maxKeyBytes > 0)
    // Keep their one-wave rule unchanged. A separate marker makes this one extra
    // wave inert on re-entry, including after predicate pushdown changes aliases.
    if (markedFilters(plan, marker) > 0 || markedFilters(plan, firstMarker) >= maximum) return plan
    val matches = plan.collect { case join: Join if target(join).nonEmpty => join }
    if (matches.size != 1) return plan
    val join = matches.head
    val (probeKey, buildKey) = target(join).get
    if (
      hasBloomForKey(join.left, probeKey) ||
      maxScanBytes(
        join.left) < conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
    ) return plan
    val projected = Project(Seq(Alias(buildKey, "creationKey")()), join.right)
    if (projected.stats.sizeInBytes > maxKeyBytes) return plan
    val aggregate = new BloomFilterAggregate(new XxHash64(Seq(buildKey)))
    val result = Alias(aggregate.toAggregateExpression(), marker)()
    val creation = ConstantFolding(ColumnPruning(Aggregate(Nil, Seq(result), join.right)))
    val predicate =
      BloomFilterMightContain(ScalarSubquery(creation, Nil), new XxHash64(Seq(probeKey)))
    val changed = plan.transformUp {
      case candidate: Join if candidate eq join =>
        val replacement = candidate.copy(left = Filter(predicate, candidate.left))
        replacement.copyTagsFrom(candidate)
        replacement.setTagValue(injected, true)
        replacement
    }
    logInfo(s"Q5_SECOND_HOP_BLOOM added=1 projectedKeyBytes=${projected.stats.sizeInBytes}; exact join preserved")
    PushDownPredicates(changed)
  }
}
