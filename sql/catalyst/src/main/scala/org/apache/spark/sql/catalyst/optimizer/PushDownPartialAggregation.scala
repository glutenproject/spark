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
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
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


  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(JOIN), ruleId) {
        case agg @ PartialAggregate(_, _, j: Join)
          if j.children.exists(_.isInstanceOf[AggregateBase]) =>
          agg
        case agg @ PartialAggregate(groupings, aggs, Project(_,
        j @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, _, _, left, right, _)))
          if canPlanAsBroadcastHashJoin(j, conf) &&
            !j.children.exists(_.isInstanceOf[AggregateBase]) =>
          val aggRefs = AttributeSet(agg.collectAggregateExprs.flatMap(_.references))
          if (aggRefs.subsetOf(j.left.outputSet) && leftKeys.forall(_.isInstanceOf[Attribute])) {
            val pushedLeft = constructPartialAgg(
              leftKeys.map(_.asInstanceOf[Attribute]),
              groupings.filter(_.references.subsetOf(left.outputSet))
                .map(_.asInstanceOf[Attribute]),
              aggs.filter(_.references.subsetOf(left.outputSet)),
              left)
            val newChild = Project(aggs.map(_.toAttribute), j.copy(left = pushedLeft))
            newChild
          } else if (aggRefs.subsetOf(j.right.outputSet)) {
            val pushedRight = constructPartialAgg(
              rightKeys.map(_.asInstanceOf[Attribute]),
              groupings.filter(_.references.subsetOf(right.outputSet))
                .map(_.asInstanceOf[Attribute]),
              aggs.filter(_.references.subsetOf(right.outputSet)),
              right)
            val newChild = Project(aggs.map(_.toAttribute), j.copy(right = pushedRight))
            newChild
          } else {
            agg
          }
      }
    }
  }
}
