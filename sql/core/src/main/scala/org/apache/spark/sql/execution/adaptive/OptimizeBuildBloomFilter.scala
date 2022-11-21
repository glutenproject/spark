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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.BUILD_BLOOM_FILTER
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

object OptimizeBuildBloomFilter extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsPattern(BUILD_BLOOM_FILTER), ruleId) {
        case fa @ ObjectHashAggregateExec(Some(_), false, _, Nil,
               Seq(fae @ AggregateExpression(fb: BloomFilterAggregate, _, _, _, _)), _, _, _,
                 se @ ShuffleExchangeExec(_,
             pa @ ObjectHashAggregateExec(None, false, _, Nil,
               Seq(pae @ AggregateExpression(_: BloomFilterAggregate, _, _, _, _)), _, _, _,
                 s: ShuffleQueryStageExec), _)) if s.isMaterialized && s.mapStats.isDefined =>
          val bf = new BloomFilterAggregate(fb.child, s.getRuntimeStatistics.rowCount.get.toLong)
          val newPartialAgg = pa.copy(aggregateExpressions = Seq(pae.copy(aggregateFunction = bf)))
          val newFinalAgg = fa.copy(aggregateExpressions = Seq(fae.copy(aggregateFunction = bf)))
          newFinalAgg.copy(child = se.copy(child = newPartialAgg))
      }
    }
  }
}
