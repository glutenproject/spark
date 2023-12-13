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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Insert a filter on one side of the join if the other side has a selective predicate.
 * The filter could be an IN subquery (converted to a semi join), a bloom filter, or something
 * else in the future.
 */
object InjectRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  // Wraps `expr` with a hash function if its byte size is larger than an integer.
  private def mayWrapWithHash(expr: Expression): Expression = {
    if (expr.dataType.defaultSize > IntegerType.defaultSize) {
      new Murmur3Hash(Seq(expr))
    } else {
      expr
    }
  }

  private def injectFilter(
      filterApplicationSideExp: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideExp: Expression,
      filterCreationSidePlan: LogicalPlan): LogicalPlan = {
    require(conf.runtimeFilterBloomFilterEnabled || conf.runtimeFilterSemiJoinReductionEnabled)
    if (conf.runtimeFilterBloomFilterEnabled) {
      injectBloomFilter(
        filterApplicationSideExp,
        filterApplicationSidePlan,
        filterCreationSideExp,
        filterCreationSidePlan
      )
    } else {
      injectInSubqueryFilter(
        filterApplicationSideExp,
        filterApplicationSidePlan,
        filterCreationSideExp,
        filterCreationSidePlan
      )
    }
  }

  // Estimate the ratio of selective filters.
  def estimateSelectiveFilteringRatio(buildPlan: LogicalPlan): Double = {
    def estimateWithDistinctCount(expr: Expression): Option[Double] = {
      val distinctCountOption = expr.references.toList match {
        case attr :: Nil =>
          distinctCounts(attr, buildPlan)
        case _ => None
      }

      distinctCountOption.map {
        case distinctCount if distinctCount > 0 => 1 / distinctCount.toDouble
      }
    }

    def estimate(expr: Expression, plan: LogicalPlan): Double = expr match {
      case a EqualNullSafe b
        if b.foldable && plan.output.exists(_.semanticEquals(a)) =>
        estimateWithDistinctCount(a).getOrElse(0.1)
      case a EqualNullSafe b
        if a.foldable && plan.output.exists(_.semanticEquals(b)) =>
        estimateWithDistinctCount(b).getOrElse(0.1)
      case a EqualTo b
        if b.foldable && plan.output.exists(_.semanticEquals(a)) =>
        estimateWithDistinctCount(a).getOrElse(0.1)
      case a EqualTo b
        if a.foldable && plan.output.exists(_.semanticEquals(b)) =>
        estimateWithDistinctCount(b).getOrElse(0.1)
      case _: BinaryComparison => 0.7
      case InSet(v, hset: Set[Any]) if plan.output.exists(_.semanticEquals(v)) =>
        estimateWithDistinctCount(v).getOrElse {
          if (hset.size > 100) {
            0.9
          } else if (hset.size > 50) {
            0.8
          } else if (hset.size > 30) {
            0.7
          } else if (hset.size > 15) {
            0.6
          } else if (hset.size > 10) {
            0.5
          } else if (hset.size > 5) {
            0.4
          } else {
            0.3
          }
        }
      case In(v, list: Seq[Expression]) if plan.output.exists(_.semanticEquals(v)) =>
        estimateWithDistinctCount(v).getOrElse {
          if (list.size > 100) {
            0.9
          } else if (list.size > 50) {
            0.8
          } else if (list.size > 30) {
            0.7
          } else if (list.size > 15) {
            0.6
          } else if (list.size > 10) {
            0.5
          } else if (list.size > 5) {
            0.4
          } else {
            0.3
          }
        }
      case And(l, r) =>
        estimate(l, plan) * estimate(r, plan)
      case Or(l, r) =>
        estimate(l, plan) + estimate(r, plan)
      case DynamicPruningSubquery(pruningKey, buildPlan, buildKeys, index, _, _) =>
        val buildKey = buildKeys(index)
        val filterRatio =
          estimateFilteringRatio(pruningKey, plan, buildKey, buildPlan, conf)
        val dynamicPruningRatio = estimateSelectiveFilteringRatio(buildPlan)
        (1 - filterRatio) * dynamicPruningRatio
      case BloomFilterMightContain(ScalarSubquery(
        Aggregate(_, Seq(Alias(AggregateExpression(
        BloomFilterAggregate(XxHash64Key(buildKey), _, _, _, _), _, _, _, _),
        _)), buildPlan), _, _, _), XxHash64Key(pruningKey)) =>
        val filterRatio =
          estimateFilteringRatio(pruningKey, plan, buildKey, buildPlan, conf)
        val runtimeFilteringRatio = estimateSelectiveFilteringRatio(buildPlan)
        (1 - filterRatio) * runtimeFilteringRatio *
          conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_PREDICATE_ADJUST_FACTOR)
      case _ => 0.9
    }

    val predicateRatios = buildPlan.collect {
      case Filter(condition, child) =>
        splitConjunctivePredicates(condition).map { predicate =>
          estimate(predicate, child)
        }
    }.flatten

    predicateRatios.reduce(_ * _)
  }

  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    val ratio = estimateSelectiveFilteringRatio(plan)

    conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_STATISTICS_ADJUST_FACTOR) *
      ratio * plan.stats.sizeInBytes.toDouble <=
      conf.runtimeFilterCreationSideThreshold
  }

  private def injectBloomFilter(
      filterApplicationSideExp: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideExp: Expression,
      filterCreationSidePlan: LogicalPlan): LogicalPlan = {
    // Skip if the filter creation side is too big
    filterCreationSidePlan match {
      case ProjectAdapter(_, child) =>
        if (!hasSelectivePredicate(child)) {
          return filterApplicationSidePlan
        }
      case _ =>
        if (!hasSelectivePredicate(filterCreationSidePlan)) {
          return filterApplicationSidePlan
        }
    }
    val rowCount = filterCreationSidePlan.stats.rowCount
    val bloomFilterAgg =
      if (rowCount.isDefined && rowCount.get.longValue > 0L) {
        new BloomFilterAggregate(new XxHash64(Seq(filterCreationSideExp)),
          Literal(rowCount.get.longValue))
      } else {
        new BloomFilterAggregate(new XxHash64(Seq(filterCreationSideExp)))
      }

    val alias = Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()
    val aggregate =
      ConstantFolding(ColumnPruning(Aggregate(Nil, Seq(alias), filterCreationSidePlan)))
    val bloomFilterSubquery = filterCreationSidePlan match {
      case _: ProjectAdapter =>
        // Try to reuse the results of exchange.
        RuntimeFilterSubquery(filterApplicationSideExp, aggregate, filterCreationSideExp)
      case _ =>
        ScalarSubquery(aggregate, Nil)
    }
    val filter = BloomFilterMightContain(bloomFilterSubquery,
      new XxHash64(Seq(filterApplicationSideExp)))
    Filter(filter, filterApplicationSidePlan)
  }

  private def injectInSubqueryFilter(
      filterApplicationSideExp: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideExp: Expression,
      filterCreationSidePlan: LogicalPlan): LogicalPlan = {
    require(filterApplicationSideExp.dataType == filterCreationSideExp.dataType)
    val actualFilterKeyExpr = mayWrapWithHash(filterCreationSideExp)
    val alias = Alias(actualFilterKeyExpr, actualFilterKeyExpr.toString)()
    val aggregate = ColumnPruning(Aggregate(Seq(alias), Seq(alias), filterCreationSidePlan))
    if (!canBroadcastBySize(aggregate, conf)) {
      // Skip the InSubquery filter if the size of `aggregate` is beyond broadcast join threshold,
      // i.e., the semi-join will be a shuffled join, which is not worthwhile.
      return filterApplicationSidePlan
    }
    val filter = InSubquery(Seq(mayWrapWithHash(filterApplicationSideExp)),
      ListQuery(aggregate, childOutputs = aggregate.output))
    Filter(filter, filterApplicationSidePlan)
  }

  /**
   * Extracts a sub-plan which is a simple filter over scan from the input plan. The simple
   * filter should be selective and the filter condition (including expressions in the child
   * plan referenced by the filter condition) should be a simple expression, so that we do
   * not add a subquery that might have an expensive computation.
   */
  private def extractSelectiveFilterOverScan(
      plan: LogicalPlan,
      filterCreationSideExp: Expression): Option[(Expression, LogicalPlan)] = {

    /**
     * Find the passed creation side expression.
     *
     * Note: If one side of the join condition contains the current creation side expression,
     * the expression on the other side of the join condition can be used as
     * the passed creation side.
     */
    def findPassedFilterCreationSideExp(
        joinKeys: Seq[Expression],
        otherJoinKeys: Seq[Expression],
        filterCreationSideExp: Expression): Option[Expression] = {
      joinKeys.zipWithIndex.find { case (key, _) =>
        filterCreationSideExp.semanticEquals(key)
      }.map { case (_, idx) =>
        otherJoinKeys(idx)
      }
    }

    def existsFilteringSubquery(plan: LogicalPlan): Boolean = {
      plan.exists {
        case Filter(condition, _) =>
          splitConjunctivePredicates(condition).exists {
            case _: DynamicPruningSubquery => true
            case _: BloomFilterMightContain => true
            case _ => false
          }
        case _ => false
      }
    }

    def extract(
        p: LogicalPlan,
        predicateReference: AttributeSet,
        hasHitFilter: Boolean,
        hasHitSelectiveFilter: Boolean,
        currentPlan: LogicalPlan,
        currentFilterCreationSideExp: Expression): Option[(Expression, LogicalPlan)] = p match {
      case Project(projectList, child) if hasHitFilter =>
        // We need to make sure all expressions referenced by filter predicates are simple
        // expressions.
        val referencedExprs = projectList.filter(predicateReference.contains)
        if (referencedExprs.forall(isSimpleExpression)) {
          extract(
            child,
            referencedExprs.map(_.references).foldLeft(AttributeSet.empty)(_ ++ _),
            hasHitFilter,
            hasHitSelectiveFilter,
            currentPlan,
            currentFilterCreationSideExp)
        } else {
          None
        }
      case Project(_, child) =>
        assert(predicateReference.isEmpty && !hasHitSelectiveFilter)
        extract(child, predicateReference, hasHitFilter, hasHitSelectiveFilter, currentPlan,
          currentFilterCreationSideExp)
      case Filter(condition, child) if isSimpleExpression(condition) =>
        if (conf.runtimeFilterBloomFilterEnabled && conf.adaptiveExecutionEnabled) {
          val (filteringSubquerys, otherPredicates) =
            splitConjunctivePredicates(condition).partition {
              case _: DynamicPruningSubquery => true
              case BloomFilterMightContain(_: ScalarSubquery, _) => true
              case _ => false
            }

          val existsLikelySelective = otherPredicates.exists(isLikelySelective)
          val extracted = extract(
            child,
            predicateReference ++ condition.references,
            hasHitFilter = true,
            hasHitSelectiveFilter = hasHitSelectiveFilter || existsLikelySelective ||
              filteringSubquerys.nonEmpty,
            currentPlan,
            currentFilterCreationSideExp)
          if (conf.exchangeReuseEnabled) {
            extracted.map {
              case (expr, plan) if existsFilteringSubquery(plan) =>
                (expr, ProjectAdapter(plan.output, plan))
              case other =>
                other
            }
          } else {
            extracted
          }
        } else {
          extract(
            child,
            predicateReference ++ condition.references,
            hasHitFilter = true,
            hasHitSelectiveFilter = hasHitSelectiveFilter || isLikelySelective(condition),
            currentPlan,
            currentFilterCreationSideExp)
        }
      case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, left, right, _) =>
        // Runtime filters use one side of the [[Join]] to build a set of join key values and prune
        // the other side of the [[Join]]. It's also OK to use a superset of the join key values
        // (ignore null values) to do the pruning.
        if (left.output.exists(_.semanticEquals(currentFilterCreationSideExp))) {
          val extracted = extract(left, AttributeSet.empty,
            hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = left,
            currentFilterCreationSideExp = currentFilterCreationSideExp)

          // Prioritize the use of runtime filter based on simple selective predicates and
          // avoid shuffle.
          if (extracted.isEmpty || extracted.exists(_._2.isInstanceOf[ProjectAdapter])) {
            // There may be a passing of the creation side expression for runtime filters.
            val extractWithPassedKey =
              findPassedFilterCreationSideExp(lkeys, rkeys, currentFilterCreationSideExp)
                .flatMap { passedFilterCreationSideExp =>
                  extract(right, AttributeSet.empty,
                    hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = right,
                    currentFilterCreationSideExp = passedFilterCreationSideExp)
                }.filter(!_._2.isInstanceOf[ProjectAdapter])

            if (extractWithPassedKey.isEmpty) {
              extracted
            } else {
              extractWithPassedKey
            }
          } else {
            extracted
          }
        } else if (right.output.exists(_.semanticEquals(currentFilterCreationSideExp))) {
          val extracted = extract(right, AttributeSet.empty,
            hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = right,
            currentFilterCreationSideExp = currentFilterCreationSideExp)
          // Prioritize the use of runtime filter based on simple selective predicates and
          // avoid shuffle.
          if (extracted.isEmpty || extracted.exists(_._2.isInstanceOf[ProjectAdapter])) {
            // There may be a passing of the creation side expression for runtime filters.
            val extractWithPassedKey =
              findPassedFilterCreationSideExp(rkeys, lkeys, currentFilterCreationSideExp)
                .flatMap { passedFilterCreationSideExp =>
                  extract(left, AttributeSet.empty,
                    hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = left,
                    currentFilterCreationSideExp = passedFilterCreationSideExp)
                }.filter(!_._2.isInstanceOf[ProjectAdapter])

            if (extractWithPassedKey.isEmpty) {
              extracted
            } else {
              extractWithPassedKey
            }
          } else {
            extracted
          }
        } else {
          None
        }
      case _: LeafNode if hasHitSelectiveFilter =>
        Some((currentFilterCreationSideExp, currentPlan))
      case _ => None
    }

    if (!plan.isStreaming) {
      extract(plan, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
        currentPlan = plan, currentFilterCreationSideExp = filterCreationSideExp)
    } else {
      None
    }
  }

  private def isSimpleExpression(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY,
      REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE)
  }

  private def isProbablyShuffleJoin(left: LogicalPlan,
      right: LogicalPlan, hint: JoinHint): Boolean = {
    !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) &&
      !canBroadcastBySize(left, conf) && !canBroadcastBySize(right, conf)
  }

  private def probablyHasShuffle(plan: LogicalPlan): Boolean = {
    plan.exists {
      case Join(left, right, _, _, hint) => isProbablyShuffleJoin(left, right, hint)
      case _: Aggregate => true
      case _: Window => true
      case _ => false
    }
  }

  // Returns the max scan byte size in the subtree rooted at `filterApplicationSide`.
  private def maxScanByteSize(filterApplicationSide: LogicalPlan): BigInt = {
    val defaultSizeInBytes = conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    filterApplicationSide.collect({
      case leaf: LeafNode => leaf
    }).map(scan => {
      // DEFAULT_SIZE_IN_BYTES means there's no byte size information in stats. Since we avoid
      // creating a Bloom filter when the filter application side is very small, so using 0
      // as the byte size when the actual size is unknown can avoid regression by applying BF
      // on a small table.
      if (scan.stats.sizeInBytes == defaultSizeInBytes) BigInt(0) else scan.stats.sizeInBytes
    }).max
  }

  // Returns true if `filterApplicationSide` satisfies the byte size requirement to apply a
  // Bloom filter; false otherwise.
  private def satisfyByteSizeRequirement(filterApplicationSide: LogicalPlan): Boolean = {
    // In case `filterApplicationSide` is a union of many small tables, disseminating the Bloom
    // filter to each small task might be more costly than scanning them itself. Thus, we use max
    // rather than sum here.
    val maxScanSize = maxScanByteSize(filterApplicationSide)
    maxScanSize >=
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
  }

  /**
   * Extracts the beneficial filter creation plan with check show below:
   * - The filterApplicationSideJoinExp can be pushed down through joins, aggregates and windows
   *   (ie the expression references originate from a single leaf node)
   * - The filter creation side has a selective predicate
   * - The max filterApplicationSide scan size is greater than a configurable threshold
   */
  private def extractBeneficialFilterCreatePlan(
      filterApplicationSide: LogicalPlan,
      filterCreationSide: LogicalPlan,
      filterApplicationSideExp: Expression,
      filterCreationSideExp: Expression): Option[(Expression, LogicalPlan)] = {
    if (findExpressionAndTrackLineageDown(
      filterApplicationSideExp, filterApplicationSide).isDefined &&
      satisfyByteSizeRequirement(filterApplicationSide)) {
      extractSelectiveFilterOverScan(filterCreationSide, filterCreationSideExp)
    } else {
      None
    }
  }

  def hasRuntimeFilter(left: LogicalPlan, right: LogicalPlan, leftKey: Expression,
      rightKey: Expression): Boolean = {
    if (conf.runtimeFilterBloomFilterEnabled) {
      hasBloomFilter(left, right, leftKey, rightKey)
    } else {
      hasInSubquery(left, right, leftKey, rightKey)
    }
  }

  def hasDynamicPruningSubquery(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    left.exists {
      case Filter(condition, plan) =>
        splitConjunctivePredicates(condition).exists {
          case DynamicPruningSubquery(pruningKey, _, _, _, _, _) =>
            pruningKey.fastEquals(leftKey) ||
              hasDynamicPruningSubquery(plan, right, leftKey, rightKey)
          case _ => false
        }
      case _ => false
    } || right.exists {
      case Filter(condition, plan) =>
        splitConjunctivePredicates(condition).exists {
          case DynamicPruningSubquery(pruningKey, _, _, _, _, _) =>
            pruningKey.fastEquals(rightKey) ||
              hasDynamicPruningSubquery(left, plan, leftKey, rightKey)
          case _ => false
        }
      case _ => false
    }
  }

  def hasBloomFilter(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    findBloomFilterWithExp(left, leftKey) || findBloomFilterWithExp(right, rightKey)
  }

  private def findBloomFilterWithExp(plan: LogicalPlan, key: Expression): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case BloomFilterMightContain(_, XxHash64(Seq(valueExpression), _))
            if valueExpression.fastEquals(key) => true
          case _ => false
        }
      case _ => false
    }
  }

  def hasInSubquery(left: LogicalPlan, right: LogicalPlan, leftKey: Expression,
      rightKey: Expression): Boolean = {
    findInSubqueryWithExp(left, leftKey) || findInSubqueryWithExp(right, rightKey)
  }

  private def findInSubqueryWithExp(plan: LogicalPlan, key: Expression): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case InSubquery(Seq(value),
            ListQuery(Aggregate(Seq(Alias(_, _)), Seq(Alias(_, _)), _), _, _, _, _)) =>
            value.fastEquals(key) || value.fastEquals(new Murmur3Hash(Seq(key)))
          case _ => false
        }
      case _ => false
    }
  }

  private def tryInjectRuntimeFilter(
      plan: LogicalPlan, initialFilterCount: Int = 0): (LogicalPlan, Int) = {
    var filterCounter = initialFilterCount
    val numFilterThreshold = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD)
    val newPlan = plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        (leftKeys, rightKeys).zipped.foreach((l, r) => {
          // Check if:
          // 1. There is already a DPP filter on the key
          // 2. There is already a runtime filter (Bloom filter or IN subquery) on the key
          // 3. The keys are simple cheap expressions
          if (filterCounter < numFilterThreshold &&
            !hasDynamicPruningSubquery(left, right, l, r) &&
            !hasRuntimeFilter(newLeft, newRight, l, r) &&
            isSimpleExpression(l) && isSimpleExpression(r)) {
            val oldLeft = newLeft
            val oldRight = newRight
            // Check if the current join is a shuffle join or a broadcast join that
            // has a shuffle below it
            val hasShuffle = isProbablyShuffleJoin(left, right, hint)
            if (canPruneLeft(joinType) && (hasShuffle || probablyHasShuffle(left))) {
              extractBeneficialFilterCreatePlan(left, right, l, r).foreach {
                case (filterCreationSideExp, filterCreationSidePlan) =>
                  newLeft = injectFilter(l, newLeft, filterCreationSideExp, filterCreationSidePlan)
              }
            }
            // Did we actually inject on the left? If not, try on the right
            // Check if the current join is a shuffle join or a broadcast join that
            // has a shuffle below it
            if (newLeft.fastEquals(oldLeft) && canPruneRight(joinType) &&
              (hasShuffle || probablyHasShuffle(right))) {
              extractBeneficialFilterCreatePlan(right, left, r, l).foreach {
                case (filterCreationSideExp, filterCreationSidePlan) =>
                  newRight =
                    injectFilter(r, newRight, filterCreationSideExp, filterCreationSidePlan)
              }
            }
            if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
              filterCounter = filterCounter + 1
            }
          }
        })
        join.withNewChildren(Seq(newLeft, newRight))
    }

    (newPlan, filterCounter)
  }

  private def pushDownPredicates(plan: LogicalPlan): LogicalPlan = {
    // The Max number of push down attempts.
    val maxAttempts = conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_PUSHDOWN_PREDICATES_ATTEMPTS)
    // We can't inject bloom filter with bloom filters are already injected if there are still
    // exists some bloom filter not be pushed down.
    var tries = 0
    var pushedPlan = plan
    while (tries < maxAttempts) {
      tries += 1
      pushedPlan = PushDownPredicates(pushedPlan)
    }
    pushedPlan
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ if !conf.runtimeFilterSemiJoinReductionEnabled &&
      !conf.runtimeFilterBloomFilterEnabled => plan
    case _ =>
      // Try to inject runtime filter based on simple and selective predicates or
      // dynamic pruning subquery.
      val (newPlan, currentFilterCount) = tryInjectRuntimeFilter(plan)
      val finalPlan = if (conf.adaptiveExecutionEnabled) {
        // Push down the predicates of runtime filter.
        val pushedPlan = pushDownPredicates(newPlan)
        // Try to inject runtime filter based on bloom filter subquery.
        tryInjectRuntimeFilter(pushedPlan, currentFilterCount)._1
      } else {
        newPlan
      }
      if (conf.runtimeFilterSemiJoinReductionEnabled && !plan.fastEquals(finalPlan)) {
        RewritePredicateSubquery(finalPlan)
      } else {
        finalPlan
      }
  }

}
