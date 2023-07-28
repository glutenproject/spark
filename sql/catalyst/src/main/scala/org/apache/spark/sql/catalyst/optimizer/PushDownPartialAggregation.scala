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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.PushPartialAggregationThroughJoin.pushPartialAggHasBenefit
import org.apache.spark.sql.catalyst.planning.ExtractPushablePartialAggAndJoins
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

object PushDownPartialAggregation extends Rule[LogicalPlan]
  with JoinSelectionHelper
  with PredicateHelper {

  private def deduplicateNamedExpressions(
      aggregateExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    ExpressionSet(aggregateExpressions).toSeq.map(_.asInstanceOf[NamedExpression])
  }

  private def constructPartialAgg(
      joinKeys: Seq[Attribute],
      groupExps: Seq[NamedExpression],
      remainingExps: Seq[NamedExpression],
      plan: LogicalPlan): PartialAggregate = {
    val partialGroupingExps = ExpressionSet(joinKeys ++ groupExps).toSeq
    val partialAggExps = joinKeys ++ groupExps ++ remainingExps
    PartialAggregate(partialGroupingExps, deduplicateNamedExpressions(partialAggExps), plan)
  }

  private def pushDownPartialAggregation(
      groupExps: Seq[Attribute],
      leftKeys: Seq[Attribute],
      rightKeys: Seq[Attribute],
      agg: PartialAggregate,
      join: Join): LogicalPlan = {
    val aggRefs = AttributeSet(agg.collectAggregateExprs.flatMap(_.references))
    val canPushLeft = aggRefs.subsetOf(join.left.outputSet) && canPruneRight(join.joinType)
    val canPushRight = aggRefs.subsetOf(join.right.outputSet) && canPruneLeft(join.joinType)
    lazy val pushedLeft = constructPartialAgg(
      leftKeys,
      groupExps.filter(_.references.subsetOf(join.left.outputSet)),
      agg.aggregateExpressions.filter(_.references.subsetOf(join.left.outputSet)),
      join.left)
    lazy val pushedRight = constructPartialAgg(
      rightKeys,
      groupExps.filter(_.references.subsetOf(join.right.outputSet)),
      agg.aggregateExpressions.filter(_.references.subsetOf(join.right.outputSet)),
      join.right)

    if (canPushLeft && pushPartialAggHasBenefit(
      leftKeys ++ groupExps.filter(_.references.subsetOf(join.left.outputSet)), join.left,
      canPlanAsBroadcastHashJoin(join, conf))) {
      Project(agg.aggregateExpressions.map(_.toAttribute), join.copy(left = pushedLeft))
    } else if (canPushRight && pushPartialAggHasBenefit(
      rightKeys ++ groupExps.filter(_.references.subsetOf(join.right.outputSet)), join.right,
      canPlanAsBroadcastHashJoin(join, conf))) {
      Project(agg.aggregateExpressions.map(_.toAttribute), join.copy(right = pushedRight))
    } else {
      agg
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(JOIN), ruleId) {
        case agg @ PartialAggregate(_, _, j: Join)
          if j.children.exists(_.isInstanceOf[AggregateBase]) =>
          agg
        case ExtractPushablePartialAggAndJoins(groupExps, leftKeys, rightKeys, agg, j) =>
          pushDownPartialAggregation(groupExps, leftKeys, rightKeys, agg, j)
      }
    }
  }
}
