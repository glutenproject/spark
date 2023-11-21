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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{RuntimeFilterExpression, RuntimeFilterSubquery}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.RUNTIME_FILTER_SUBQUERY
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}

/**
 * This planner rule aims at rewriting runtime filter in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
 */
case class PlanRuntimeFilterFilters(sparkSession: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(_.containsPattern(RUNTIME_FILTER_SUBQUERY)) {
      case RuntimeFilterSubquery(_, buildPlan, buildKey, exprId) =>
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        val filterCreationSidePlan = getFilterCreationSidePlan(sparkPlan)
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)

        val bloomFilterSubquery = if (conf.exchangeReuseEnabled) {

          val exchange = plan collectFirst {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _)
              if left.sameResult(filterCreationSidePlan) =>
              left
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _)
              if right.sameResult(filterCreationSidePlan) =>
              right
            case SortMergeJoinExec(_, _, _, _, left, _, _)
              if left.sameResult(filterCreationSidePlan) =>
              left
            case SortMergeJoinExec(_, _, _, _, _, right, _)
              if right.sameResult(filterCreationSidePlan) =>
              right
            case exchange: Exchange
              if exchange.sameResult(filterCreationSidePlan) =>
              exchange
            case exchange: ShuffleExchangeExec
              if exchange.child.sameResult(filterCreationSidePlan) &&
                exchange.output.exists(_.semanticEquals(buildKey)) =>
              ShuffleExchangeExec(
                exchange.outputPartitioning, exchange.child, exchange.shuffleOrigin)
          }

          if (exchange.isDefined) {
            exchange.get.setLogicalLink(filterCreationSidePlan.logicalLink.get)

            val newExecutedPlan = executedPlan transformUp {
              case p @ ProjectExec(_, inputAdapter: InputAdapter)
                if inputAdapter.child.canonicalized == exchange.get.canonicalized =>
                val newInputAdapter = inputAdapter.withNewChildren(Seq(exchange.get))
                p.withNewChildren(Seq(newInputAdapter))
            }

            ScalarSubquery(
              SubqueryExec.createForScalarSubquery(
                s"scalar-subquery#${exprId.id}",
                newExecutedPlan), exprId)
          } else {
            ScalarSubquery(
              SubqueryExec.createForScalarSubquery(
                s"scalar-subquery#${exprId.id}",
                executedPlan), exprId)
          }
        } else {
          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              executedPlan), exprId)
        }

        RuntimeFilterExpression(bloomFilterSubquery)
    }
  }

  private def getFilterCreationSidePlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        getFilterCreationSidePlan(objectHashAggregate.child)
      case shuffleExchange: ShuffleExchangeExec =>
        getFilterCreationSidePlan(shuffleExchange.child)
      case ProjectExec(_, inputAdapter: InputAdapter) =>
        getFilterCreationSidePlan(inputAdapter.child)
      case other => other
    }
  }
}
