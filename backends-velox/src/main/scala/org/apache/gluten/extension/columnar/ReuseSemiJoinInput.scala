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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, IsNotNull}
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project, RepartitionByExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LOGICAL_QUERY_STAGE, REPARTITION_OPERATION}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

/** Let a semi join read a hash shuffle that its build side already requests for the same input. */
case class ReuseSemiJoinInput(session: SparkSession) extends SparkStrategy {
  private def parquetScan(scan: LogicalRelation): Boolean = scan.relation match {
    case files: HadoopFsRelation =>
      !scan.isStreaming && files.fileFormat.isInstanceOf[ParquetFileFormat]
    case _ => false
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case join @ Join(left, right, LeftSemi, Some(EqualTo(a: Attribute, b: Attribute)), _)
        if session.sessionState.conf.exchangeReuseEnabled && right.containsAnyPattern(
          REPARTITION_OPERATION,
          LOGICAL_QUERY_STAGE) =>
      val filtered = left match {
        case Project(columns, child) if columns.forall(_.isInstanceOf[Attribute]) => child
        case other => other
      }
      filtered match {
        case Filter(IsNotNull(key: Attribute), scan: LogicalRelation)
            if parquetScan(scan) && left.outputSet.contains(key) &&
              ((key.semanticEquals(a) && right.outputSet.contains(b)) ||
                (key.semanticEquals(b) && right.outputSet.contains(a))) =>
          // An equality semi join always drops null probe keys. The same scan can therefore
          // supply either the filtered or the unfiltered shuffle without changing its result.
          val unfiltered = if (left eq filtered) scan else left.withNewChildren(Seq(scan))
          def equivalentShuffle(node: LogicalPlan): Option[RepartitionByExpression] = node match {
            case donor: RepartitionByExpression
                if donor.optNumPartitions.isDefined &&
                  donor.partitionExpressions.size == 1 &&
                  donor.partitionExpressions.head.isInstanceOf[Attribute] =>
              Iterator(left, unfiltered)
                .map(input => donor.copy(partitionExpressions = Seq(key), child = input))
                .find(_.sameResult(donor))
            case stage: LogicalQueryStage =>
              stage.logicalPlan.collectFirst(Function.unlift(equivalentShuffle))
            case _ => None
          }
          right.collectFirst(Function.unlift(equivalentShuffle)).toList.map {
            replacement => planLater(join.copy(left = replacement))
          }
        case _ => Nil
      }
    case _ => Nil
  }
}
