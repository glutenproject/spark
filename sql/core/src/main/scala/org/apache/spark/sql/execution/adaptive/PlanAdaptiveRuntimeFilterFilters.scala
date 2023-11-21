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

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Literal, RuntimeFilterExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER}
import org.apache.spark.sql.execution.{InputAdapter, ProjectExec, ScalarSubquery, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec, SubqueryExec, SubqueryWrapper}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec, SubqueryBroadcastExecProxy}

/**
 * A rule to insert runtime filter in order to reuse exchange.
 */
case class PlanAdaptiveRuntimeFilterFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER)) {
      case filterMightContain @ BloomFilterMightContain(RuntimeFilterExpression(SubqueryWrapper(
          SubqueryAdaptiveBroadcastExec(name, index, true, _, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId)), _) =>
        val filterCreationSidePlan = getFilterCreationSidePlan(adaptivePlan.executedPlan)

        if (conf.exchangeReuseEnabled && buildKeys.nonEmpty) {
          val optionalExchange = collectFirst(rootPlan) {
            case exchange: BroadcastExchangeExec
              if exchange.child.sameResult(filterCreationSidePlan) &&
                buildKeys.forall(k => exchange.output.exists(_.semanticEquals(k))) =>

              BroadcastExchangeExec(exchange.mode, filterCreationSidePlan)
            case exchange: ShuffleExchangeExec
              if exchange.child.sameResult(filterCreationSidePlan) &&
                buildKeys.forall(k => exchange.output.exists(_.semanticEquals(k))) =>
              ShuffleExchangeExec(
                exchange.outputPartitioning, exchange.child, exchange.shuffleOrigin)
          }

          if (optionalExchange.isDefined) {
            val exchange = optionalExchange.get
            exchange.setLogicalLink(filterCreationSidePlan.logicalLink.get)

            val newExecutedPlan = adaptivePlan.executedPlan transformUp {
              case p @ ProjectExec(_, inputAdapter: InputAdapter)
                if inputAdapter.child.canonicalized == exchange.child.canonicalized =>
                val exchangeProxy = exchange match {
                  case e: BroadcastExchangeExec =>
                    val broadcastValues = SubqueryBroadcastExec(name, index, buildKeys, e)
                    val newOutput =
                      filterCreationSidePlan.output.filter(_.semanticEquals(buildKeys(index)))
                    SubqueryBroadcastExecProxy(broadcastValues, newOutput)
                  case other => other
                }
                val newInputAdapter = inputAdapter.withNewChildren(Seq(exchangeProxy))
                p.withNewChildren(Seq(newInputAdapter))
            }
            val newAdaptivePlan = adaptivePlan.copy(inputPlan = newExecutedPlan)

            val scalarSubquery = ScalarSubquery(
              SubqueryExec.createForScalarSubquery(
                s"scalar-subquery#${exprId.id}",
                newAdaptivePlan), exprId)
            filterMightContain.withNewChildren(
              Seq(RuntimeFilterExpression(scalarSubquery), filterMightContain.valueExpression))
          } else {
            RuntimeFilterExpression(Literal.TrueLiteral)
          }
        } else {
          RuntimeFilterExpression(Literal.TrueLiteral)
        }
    }
  }

  private def getFilterCreationSidePlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        getFilterCreationSidePlan(objectHashAggregate.child)
      case shuffleExchange: ShuffleExchangeExec =>
        getFilterCreationSidePlan(shuffleExchange.child)
      case queryStageExec: ShuffleQueryStageExec =>
        getFilterCreationSidePlan(queryStageExec.plan)
      case ProjectExec(_, inputAdapter: InputAdapter) =>
        getFilterCreationSidePlan(inputAdapter.child)
      case inputAdapter: InputAdapter =>
        getFilterCreationSidePlan(inputAdapter.child)
      case other => other
    }
  }
}
