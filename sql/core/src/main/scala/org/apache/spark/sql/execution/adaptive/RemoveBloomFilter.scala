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

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, DynamicPruningExpression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{BLOOM_FILTER_MIGHT_CONTAIN, DYNAMIC_PRUNING_EXPRESSION}
import org.apache.spark.sql.execution.{ScalarSubquery, SparkPlan, SubqueryExec}
import org.apache.spark.sql.internal.SQLConf

object RemoveBloomFilter extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      plan
    } else {
      plan.transformAllExpressionsWithPruning(
        _.containsAllPatterns(DYNAMIC_PRUNING_EXPRESSION, BLOOM_FILTER_MIGHT_CONTAIN)) {
        case d @ DynamicPruningExpression(BloomFilterMightContain(ScalarSubquery(
            SubqueryExec(_, a: AdaptiveSparkPlanExec, _), _), _)) =>
          val exceedMaxNumItems = a.executedPlan.expressions.exists {
            case AggregateExpression(bf: BloomFilterAggregate, _, _, _, _) =>
              bf.estimatedNumItems > conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS)
            case _ => false
          }
          if (exceedMaxNumItems) DynamicPruningExpression(Literal.TrueLiteral) else d
      }
    }
  }
}
