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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, DynamicPruningExpression, Literal, XxHash64}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.{ScalarSubquery => ScalarSubqueryExec}
import org.apache.spark.sql.execution.dynamicpruning.DynamicPruningHelper
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast.
 */
case class PlanAdaptiveDynamicPruningFilters(rootPlan: AdaptiveSparkPlanExec)
  extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper
    with JoinSelectionHelper
    with DynamicPruningHelper {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(DYNAMIC_PRUNING_EXPRESSION, IN_SUBQUERY_EXEC)) {
      case DynamicPruningExpression(InSubqueryExec(
          value, SubqueryAdaptiveBroadcastExec(name, onlyInBroadcast, buildPlan, buildKey,
          adaptivePlan: AdaptiveSparkPlanExec), exprId, _, _, _)) =>
        lazy val exchangeKeys = collectFirst(rootPlan) {
          case BroadcastHashJoinExec(leftKeys, _, _, BuildLeft, _, l: BroadcastExchangeExec, _, _)
            if leftKeys.nonEmpty && l.child.sameResult(adaptivePlan.executedPlan) =>
            val buildKeys = leftKeys.map { e =>
              adaptivePlan.executedPlan.output(l.output.indexWhere(_.semanticEquals(e)))
            }
            (buildKeys, l.mode)
          case BroadcastHashJoinExec(_, rightKeys, _, BuildRight, _, _, r: BroadcastExchangeExec, _)
            if rightKeys.nonEmpty && r.child.sameResult(adaptivePlan.executedPlan) =>
            val buildKeys = rightKeys.map { e =>
              adaptivePlan.executedPlan.output(r.output.indexWhere(_.semanticEquals(e)))
            }
            (buildKeys, r.mode)
        }

        if (conf.exchangeReuseEnabled && exchangeKeys.nonEmpty) {
          val (buildKeys, exchangeMode) = exchangeKeys.get
          val exchange = BroadcastExchangeExec(exchangeMode, adaptivePlan.executedPlan)
          exchange.setLogicalLink(adaptivePlan.executedPlan.logicalLink.get)
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = exchange)

          val broadcastValues = SubqueryBroadcastExec(
            name, buildKeys.indexWhere(_.semanticEquals(buildKey)), buildKeys, newAdaptivePlan)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) {
          DynamicPruningExpression(Literal.TrueLiteral)
        } else if (canBroadcastBySize(buildPlan, conf) && false) {
          // we need to apply an aggregate on the buildPlan in order to be column pruned
          val alias = Alias(buildKey, buildKey.toString)()
          val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)

          val session = adaptivePlan.context.session
          val sparkPlan = QueryExecution.prepareExecutedPlan(
            session, aggregate, adaptivePlan.context)
          assert(sparkPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val newAdaptivePlan = sparkPlan.asInstanceOf[AdaptiveSparkPlanExec]
          val values = SubqueryExec(name, newAdaptivePlan)
          DynamicPruningExpression(InSubqueryExec(value, values, exprId))
        } else if (!conf.exchangeReuseEnabled) {
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          val childPlan = adaptivePlan.executedPlan
          val reusedShuffleExchange = collectFirst(rootPlan) {
            case s: ShuffleExchangeExec if s.child.sameResult(childPlan) => s
          }

          val bfLogicalPlan = planBloomFilterLogicalPlan(buildPlan, buildKey)
          val bfPhysicalPlan =
            planBloomFilterPhysicalPlan(bfLogicalPlan, reusedShuffleExchange).map { plan =>
              val executedPlan = QueryExecution.prepareExecutedPlan(
                adaptivePlan.context.session, plan, adaptivePlan.context)
              val scalarSubquery = ScalarSubqueryExec(SubqueryExec.createForScalarSubquery(
                s"scalar-subquery#${exprId.id}", executedPlan), exprId)
              BloomFilterMightContain(scalarSubquery, new XxHash64(value))
            }.getOrElse(Literal.TrueLiteral)
          DynamicPruningExpression(bfPhysicalPlan)
        }
    }
  }
}
