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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.{DecimalPrecision, ResolveTimeZone}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, JOIN}
import org.apache.spark.sql.types.{DecimalType, LongType, NumericType}
import org.apache.spark.sql.types.DecimalType.LongDecimal

/**
 * Push down the partial aggregation through join. It supports the following cases:
 * 1. Push down partial sum, count, avg, min, max, first and last through inner join.
 * 2. Partial deduplicate the children of join if the aggregation itself is group only.
 *
 * For example:
 * CREATE TABLE t1(a int, b int, c int) using parquet;
 * CREATE TABLE t2(x int, y int, z int) using parquet;
 * SELECT b, SUM(c) FROM t1 INNER JOIN t2 ON t1.a = t2.x GROUP BY b;
 *
 * The current optimized logical plan is:
 * Aggregate [b#2], [b#2, sum((_pushed_sum_c#13L * cnt#16L)) AS sum(c)#8L]
 * +- Project [b#2, _pushed_sum_c#13L, cnt#16L]
 *    +- Join Inner, (a#1 = x#4)
 *       :- PartialAggregate [a#1, b#2], [a#1, b#2, sum(c#3) AS _pushed_sum_c#13L]
 *       :  +- Project [b#2, c#3, a#1]
 *       :     +- Filter isnotnull(a#1)
 *       :        +- Relation default.t1[a#1,b#2,c#3] parquet
 *       +- PartialAggregate [x#4], [count(1) AS cnt#16L, x#4]
 *          +- Project [x#4]
 *             +- Filter isnotnull(x#4)
 *                +- Relation default.t2[x#4,y#5,z#6] parquet
 *
 * This rule should be applied after Join Reorder.
 */
object PushPartialAggregationThroughJoin extends Rule[LogicalPlan]
  with JoinSelectionHelper
  with PredicateHelper {

  def pushPartialAggHasBenefit(
      groupingExps: Seq[Expression],
      plan: LogicalPlan,
      isBroadcastJoin: Boolean): Boolean = {
    if (plan.stats.rowCount.exists(_ <= conf.adaptivePartialAggregationThreshold)) {
      false
    } else {
      val canReduceOutput =
        plan.stats.rowCount.forall(r => Aggregate(groupingExps, Nil, plan).stats.rowCount
          .forall(_.toDouble / r.toDouble <= conf.partialAggregationOptimizationBenefitRatio))

      if (isBroadcastJoin) {
        canReduceOutput && plan.stats.sizeInBytes > conf.autoBroadcastJoinThreshold &&
          plan.collectUntil(_.isInstanceOf[Join]).forall(_.isInstanceOf[Project]) &&
          plan.collectFirst { case j: Join => j }
            .exists { j =>
              !canPlanAsBroadcastHashJoin(j, conf) &&
                j.children.exists { c =>
                  val curGroupExps = groupingExps.filter(_.references.subsetOf(c.outputSet))
                  val joinConditions = j.condition.map(splitConjunctivePredicates).getOrElse(Nil)
                  val binaryComparisons = joinConditions.collect {
                    case b @ BinaryComparison(_: Attribute, _: Attribute) => b
                  }
                  if (binaryComparisons.size == joinConditions.size) {
                    val joinKeys = binaryComparisons.flatMap(_.children)
                      .filter(_.references.subsetOf(c.outputSet))
                    val maybePushedGroupExps = curGroupExps ++ joinKeys
                    if (maybePushedGroupExps.nonEmpty) {
                      pushPartialAggHasBenefit(maybePushedGroupExps, c, false)
                    } else {
                      true
                    }
                  } else {
                    true
                  }
                }
            }
      } else {
        canReduceOutput
      }
    }
  }

  // Returns true if `expr`'s references is non empty and can be evaluated using
  // the output of `plan`.
  private def canEvaluateOnly(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.nonEmpty && canEvaluate(expr, plan)

  // Returns true if `expr`'s references is empty or it can be evaluated using one side.
  private def canEvaluateOnOneSide(
                                    expr: Expression,
                                    leftNamedExpressions: Seq[NamedExpression],
                                    rightNamedExpressions: Seq[NamedExpression]): Boolean = {
    expr.references.subsetOf(AttributeSet(leftNamedExpressions.map(_.toAttribute))) ||
      expr.references.subsetOf(AttributeSet(rightNamedExpressions.map(_.toAttribute)))
  }

  // Splits expressions into three categories based on the attributes required to evaluate them.
  private def split(expressions: Seq[NamedExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftExprs, rest) = expressions.partition(canEvaluateOnly(_, left))
    val (rightExprs, remainingExps) = rest.partition(canEvaluateOnly(_, right))

    (leftExprs, rightExprs, remainingExps)
  }

  // Splits expressions into three categories based on the attributes required to evaluate them.
  private def splitAggregateExpressions(
      aggExps: Seq[AggregateExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftAggExprs, rest) = aggExps.partition(canEvaluateOnly(_, left))
    val (rightAggExprs, remainingAggExps) = rest.partition(canEvaluateOnly(_, right))

    (toExpressionMap(leftAggExprs), toExpressionMap(rightAggExprs), remainingAggExps)
  }

  // Convert aggregate expressions to a map, the key used to replace the current Aggregate
  // and the value used to push through Join. see the function of replaceAliasName.
  private def toExpressionMap(aggExps: Seq[AggregateExpression]) = {
    aggExps.map { a =>
      val name =
        s"_pushed_${a.aggregateFunction.prettyName}_${a.references.map(_.name).mkString("_")}"
      a.aggregateFunction.canonicalized -> Alias(a, name)()
    }.toMap[Expression, Alias]
  }

  // Replace the current Aggregate's aggregate expression references with pushed attribute:
  // 1. hasBenefit = true and otherSideCnt is not empty:
  //   - Replace the sum child with the current side pushed sum * the other side row count.
  //     e.g. sum(a) -> sum(_pushed_sum_a * cnt)
  //   - Rewrite the count child with the current side pushed count * the other side row count and
  //     sum it.
  //     e.g. count(a) -> sum(_pushed_count_a * cnt)
  // 2. hasBenefit = true and otherSideCnt is empty:
  //   - Replace the sum child with the current side pushed sum.
  //     e.g. sum(a) -> sum(_pushed_sum_a)
  //   - Rewrite the count child with the current side row count and sum it.
  //     e.g. count(a) -> sum(_pushed_count_a)
  // 3. hasBenefit = false and otherSideCnt is not empty:
  //   - Replace the sum child with the current side sum child * other side row count.
  //     e.g. sum(a) -> sum(a * cnt)
  //   - Rewrite the count child with if(child is null, 0, 1) * other side row count and sum it.
  //     e.g. count(a) -> sum(if(a is null, 0, 1) * cnt)
  private def replaceAliasName(
      expr: NamedExpression,
      aliasMap: Map[Expression, Alias],
      hasBenefit: Boolean,
      otherSideCnt: Option[Attribute]): NamedExpression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId.
    expr.mapChildren(_.transformUp {
      case e @ Sum(_, useAnsiAdd, dt) if aliasMap.contains(e.canonicalized) =>
        val resultType = e.dataType
        val countType = if (resultType.isInstanceOf[DecimalType]) LongDecimal else resultType
        val child = if (hasBenefit) aliasMap(e.canonicalized).toAttribute else e.child
        val newChild =
          otherSideCnt.map(c => Multiply(child.cast(resultType), c.cast(countType), useAnsiAdd))
            .getOrElse(child)
        Sum(newChild, useAnsiAdd, Some(dt.getOrElse(resultType)))
      case e: Count if aliasMap.contains(e.canonicalized) =>
        val child = if (hasBenefit) {
          aliasMap(e.canonicalized).toAttribute
        } else {
          If(e.children.map(IsNull).reduce(Or), Literal(0L, LongType), Literal(1L, LongType))
        }
        val newChild = otherSideCnt.map(Multiply(child, _)).getOrElse(child)
        Sum(newChild, conf.ansiEnabled, Some(e.dataType))
      case e: Min if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: Max if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: First if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: Last if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
    }).asInstanceOf[NamedExpression]
  }

  private def pushableAggExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(Sum(e, _, _), Complete, false, None, _) =>
      e.dataType.isInstanceOf[NumericType]
    case AggregateExpression(_: Min, Complete, false, None, _) => true
    case AggregateExpression(_: Max, Complete, false, None, _) => true
    case AggregateExpression(_: First, Complete, false, None, _) => true
    case AggregateExpression(_: Last, Complete, false, None, _) => true
    case AggregateExpression(Average(e, _), Complete, false, None, _) =>
      e.dataType.isInstanceOf[NumericType]
    case _ => false
  }

  // Support count(*), count(id)
  private def pushableCountExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(_: Count, Complete, false, None, _) => true
    case _ => false
  }

  private def deduplicateNamedExpressions(
      aggregateExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    ExpressionSet(aggregateExpressions).toSeq.map(_.asInstanceOf[NamedExpression])
  }

  // Rewrite Average to Sum / Count(*). Please see AverageBase.getEvaluateExpression
  private def rewriteAverage(agg: Aggregate): Aggregate = {
    if (agg.collectAggregateExprs.exists(_.aggregateFunction.isInstanceOf[Average])) {
      val newAggAggregateExpressions = agg.aggregateExpressions.map { expr =>
        expr.mapChildren(_.transformUp {
          case ae @ AggregateExpression(af, _, _, _, _) => af match {
            case avg @ Average(e, useAnsiAdd) if e.references.nonEmpty =>
              val sum = Sum(e, useAnsiAdd, Some(avg.sumDataType)).toAggregateExpression()
              val count = Count(e).toAggregateExpression()
              e.dataType match {
                case _: DecimalType =>
                  DecimalPrecision.decimalAndDecimal()(
                    Divide(
                      CheckOverflowInSum(sum, avg.sumDataType.asInstanceOf[DecimalType],
                        !useAnsiAdd), count.cast(DecimalType.LongDecimal),
                      failOnError = false)).cast(avg.dataType)
                case _ =>
                  Divide(sum.cast(avg.dataType), count.cast(avg.dataType), useAnsiAdd)
              }
            case _ => ae
          }
        }).asInstanceOf[NamedExpression]
      }

      agg.copy(aggregateExpressions = newAggAggregateExpressions)
    } else {
      agg
    }
  }

  private def pullOutJoinKeys(joinKeys: Seq[Expression]): (Seq[Attribute], ArrayBuffer[Alias]) = {
    val complexJoinKeys = new ArrayBuffer[Alias]()
    val newJoinKeys = joinKeys.map {
      case a: Attribute => a
      case o =>
        val ne = Alias(o, s"_pullout_${o.prettyName}_${o.references.map(_.name).mkString("_")}")()
        complexJoinKeys += ne
        ne.toAttribute
    }
    (newJoinKeys, complexJoinKeys)
  }

  private def constructPartialAgg(
      joinKeys: Seq[Attribute],
      groupExps: Seq[NamedExpression],
      remainingExps: Seq[NamedExpression],
      aliasMap: Map[Expression, Alias],
      rowCnt: Alias,
      plan: LogicalPlan): PartialAggregate = {
    val partialGroupingExps = ExpressionSet(joinKeys ++ groupExps).toSeq
    val partialAggExps = joinKeys ++ groupExps ++ remainingExps ++ (aliasMap.values.toSeq :+ rowCnt)
    PartialAggregate(partialGroupingExps, deduplicateNamedExpressions(partialAggExps), plan)
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

  private def pushDistinctThroughJoin(join: Join): Join = {
    var left = join.left
    var right = join.right

    val pushLeftHasBenefit =
      pushPartialAggHasBenefit(left.output, left, canPlanAsBroadcastHashJoin(join, conf))
    val pushRightHasBenefit =
      pushPartialAggHasBenefit(right.output, right, canPlanAsBroadcastHashJoin(join, conf))

    if (pushLeftHasBenefit || pushRightHasBenefit) {
      left =
        if (pushLeftHasBenefit) PartialAggregate(left.output, left.output, left) else left
      right =
        if (pushRightHasBenefit) PartialAggregate(right.output, right.output, right) else right
      join.copy(left = left, right = right)
    } else {
      join
    }
  }

  private def pushDownPartialAggregation(
      groupExps: Seq[Attribute],
      leftKeys: Seq[Attribute],
      rightKeys: Seq[Attribute],
      agg: PartialAggregate,
      join: Join): LogicalPlan = {
    val aggRefs = AttributeSet(agg.collectAggregateExprs.flatMap(_.references))
    val canPushLeft = aggRefs.subsetOf(join.left.outputSet) && canPruneRight(join.joinType) &&
      (!canPlanAsBroadcastHashJoin(join, conf) || join.left.exists {
        case j: Join => !canPlanAsBroadcastHashJoin(j, conf)
        case _ => false
      })
    val canPushRight = aggRefs.subsetOf(join.right.outputSet) && canPruneLeft(join.joinType) &&
      (!canPlanAsBroadcastHashJoin(join, conf) || join.right.exists {
        case j: Join => !canPlanAsBroadcastHashJoin(j, conf)
        case _ => false
      })
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

    val (leftGroupExps, rightGroupExps, _) =
      split(agg.groupingExpressions.map(_.asInstanceOf[Attribute]),
        join.left, join.right)

    val isPushLeftSide = canPruneRight(join.joinType) &&
      pushPartialAggHasBenefit(AttributeSet(leftKeys ++ leftGroupExps).toSeq,
        join.left, canPlanAsBroadcastHashJoin(join, conf))
    val isPushRightSide = canPruneLeft(join.joinType) &&
      pushPartialAggHasBenefit(AttributeSet(rightKeys ++ rightGroupExps).toSeq,
        join.right, canPlanAsBroadcastHashJoin(join, conf))

    if (canPushLeft && isPushLeftSide) {
      Project(agg.aggregateExpressions.map(_.toAttribute), join.copy(left = pushedLeft))
    } else if (canPushRight && isPushRightSide) {
      Project(agg.aggregateExpressions.map(_.toAttribute), join.copy(right = pushedRight))
    } else {
      agg
    }
  }

  // The entry of push down partial aggregate through join.
  // Will return the current aggregate if it can't push down.
  private def pushAggThroughJoin(
      agg: Aggregate,
      projectList: Seq[NamedExpression],
      join: Join): LogicalPlan = {
    val rewrittenAgg = rewriteAverage(agg)
    val aggregateExpressions = rewrittenAgg.collectAggregateExprs

    val joinCondition = join.condition.map(splitConjunctivePredicates)
    val joinKeys = joinCondition.getOrElse(Nil).flatMap(_.children).filterNot(_.foldable)

    val (leftKeys, rightKeys, bothSideKeys) =
      PushPredicateThroughJoin.split(joinKeys, join.left, join.right)

    val (leftProjectList, rightProjectList, remainingProjectList) =
      split(projectList ++ join.condition.map(_.references.toSeq).getOrElse(Nil),
        join.left, join.right)

    // remainingProjectList must should be empty. We do not support this case:
    // SELECT b + y, SUM(c) FROM t1 INNER JOIN t2 ON t1.a = t2.x GROUP BY 1
    //
    // Supported cases:
    // 1. groupingExpressions is not empty and aggregateExpressions are pushableAggExp or
    //    pushableCountExp
    // 2. groupingExpressions is empty and aggregateExpressions are pushableAggExp
    if (remainingProjectList.isEmpty && bothSideKeys.isEmpty &&
      ((rewrittenAgg.groupingExpressions.nonEmpty &&
        aggregateExpressions.forall(e => (pushableAggExp(e) || pushableCountExp(e)) &&
          canEvaluateOnOneSide(e, leftProjectList, rightProjectList))) ||
        (rewrittenAgg.groupingExpressions.isEmpty &&
          aggregateExpressions.forall(e => pushableAggExp(e) &&
            canEvaluateOnOneSide(e, leftProjectList, rightProjectList))))) {

      val pushedLeftProject = Project(leftProjectList, join.left)
      val pushedRightProject = Project(rightProjectList, join.right)

      // All groupingExpressions are Attributes, see PullOutGroupingExpressions.
      // Splits groupingExpressions into three categories based on the attributes.
      // We will use it as aggregateExpressions in PartialAggregate
      val (leftGroupExps, rightGroupExps, _) =
      split(rewrittenAgg.groupingExpressions.map(_.asInstanceOf[Attribute]),
        pushedLeftProject, pushedRightProject)

      val isPushLeftSide = canPruneRight(join.joinType) &&
        pushPartialAggHasBenefit(AttributeSet(leftKeys ++ leftGroupExps).toSeq,
          pushedLeftProject, canPlanAsBroadcastHashJoin(join, conf))
      val isPushRightSide = canPruneLeft(join.joinType) &&
        pushPartialAggHasBenefit(AttributeSet(rightKeys ++ rightGroupExps).toSeq,
          pushedRightProject, canPlanAsBroadcastHashJoin(join, conf))

      if (isPushLeftSide || isPushRightSide) {
        val (leftAliasMap, rightAliasMap, _) =
          splitAggregateExpressions(aggregateExpressions, pushedLeftProject, pushedRightProject)

        val remainingAggregateExps = rewrittenAgg.aggregateExpressions
          .filterNot(_.exists(_.isInstanceOf[AggregateFunction]))
        val (leftRemainingExps, rightRemainingExps, _) =
          split(remainingAggregateExps, pushedLeftProject, pushedRightProject)

        // pull out complex join condition
        val (newLeftJoinKeys, complexLeftJoinKeys) = pullOutJoinKeys(leftKeys)
        val (newRightJoinKeys, complexRightJoinKeys) = pullOutJoinKeys(rightKeys)

        val pullOutedLeft = pushedLeftProject
          .copy(projectList = deduplicateNamedExpressions(leftProjectList ++ complexLeftJoinKeys))
        val pullOutedRight = pushedRightProject
          .copy(projectList = deduplicateNamedExpressions(rightProjectList ++ complexRightJoinKeys))
        val newAliasMap = (complexLeftJoinKeys ++ complexRightJoinKeys)
          .map { a => a.child.canonicalized -> a.toAttribute }.toMap
        val newCond = joinCondition.map(_.map { singleCondition =>
          val conditionChildren = singleCondition.children.map(_.transformDown {
            case a: Attribute => a
            case e: Expression if e.foldable => e
            case e: Expression => newAliasMap.getOrElse(e.canonicalized, e)
          })
          singleCondition.withNewChildren(conditionChildren)
        }.reduceLeft(And))

        // Construct partial aggregate and rewrite current aggregate
        val cntExp = Count(Seq(Literal(1))).toAggregateExpression()
        val leftCnt = if (isPushLeftSide) Some(Alias(cntExp, "cnt")()) else None
        val rightCnt = if (isPushRightSide) Some(Alias(cntExp, "cnt")()) else None
        val leftCntAttr = leftCnt.map(_.toAttribute)
        val rightCntAttr = rightCnt.map(_.toAttribute)

        val newLeft = leftCnt.map(constructPartialAgg(newLeftJoinKeys, leftGroupExps,
          leftRemainingExps, leftAliasMap, _, pullOutedLeft)).getOrElse(pullOutedLeft)
        val newRight = rightCnt.map(constructPartialAgg(newRightJoinKeys, rightGroupExps,
          rightRemainingExps, rightAliasMap, _, pullOutedRight)).getOrElse(pullOutedRight)
        val newJoin = join.copy(
          left = newLeft,
          right = newRight,
          condition = newCond)

        val newAggregateExps = rewrittenAgg.aggregateExpressions
          .map(replaceAliasName(_, leftAliasMap, isPushLeftSide, rightCntAttr))
          .map(replaceAliasName(_, rightAliasMap, isPushRightSide, leftCntAttr))
          .map { expr =>
            expr.mapChildren(_.transformUp {
              case e @ Count(Seq(IntegerLiteral(1))) =>
                val newChild = (leftCntAttr ++ rightCntAttr)
                  .map(_.asInstanceOf[Expression]).reduceLeft(_ * _)
                Sum(newChild, conf.ansiEnabled, Some(e.dataType))
              case e @ Sum(v, useAnsiAdd, dt) if e.references.isEmpty =>
                val multiply =
                  v.cast(e.dataType) * (leftCntAttr ++ rightCntAttr)
                    .map(_.asInstanceOf[Expression]).reduceLeft(_ * _).cast(e.dataType)
                e.dataType match {
                  case decType: DecimalType =>
                    // Do not use DecimalPrecision because it may be change the precision and scale
                    Sum(CheckOverflow(multiply, decType, !useAnsiAdd), useAnsiAdd, Some(decType))
                  case _ =>
                    Sum(multiply, useAnsiAdd, Some(dt.getOrElse(e.dataType)))
                }
              // These expression do not need to rewrite:
              // Min/Max(Literal(_)), First/Last(Literal(_), _) and Average(Literal(_), _)
            }).asInstanceOf[NamedExpression]
          }

        val newAgg = if (join.joinType == Inner &&
          (ExpressionSet(leftKeys).subsetOf(ExpressionSet(leftGroupExps)) ||
            ExpressionSet(rightKeys).subsetOf(ExpressionSet(rightGroupExps)))) {
          FinalAggregate(rewrittenAgg.groupingExpressions, newAggregateExps, newJoin)
        } else {
          rewrittenAgg.copy(aggregateExpressions = newAggregateExps, child = newJoin)
        }

        ResolveTimeZone(SimplifyCasts(CollapseProject(ColumnPruning(newAgg))))
      } else {
        agg
      }
    } else {
      // We will not rewrite average if it can't push down through join.
      agg
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(AGGREGATE, JOIN), ruleId) {
        case agg @ Aggregate(_, _, j: Join)
          if j.children.exists(_.isInstanceOf[AggregateBase]) =>
          agg
        case agg @ Aggregate(_, _, Project(_, j: Join))
          if j.children.exists(_.isInstanceOf[AggregateBase]) =>
          agg

        case agg @ Aggregate(_, aggExps,
        j @ Join(_, _, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _))
          if aggExps.forall(_.deterministic) && agg.collectAggregateExprs.forall(_.isDistinct) =>
          agg.copy(child = pushDistinctThroughJoin(j))

        case agg @ Aggregate(_, aggExps, p @ Project(_,
        j @ Join(_, _, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _)))
          if aggExps.forall(_.deterministic) && agg.collectAggregateExprs.forall(_.isDistinct) =>
          agg.copy(child = p.copy(child = pushDistinctThroughJoin(j)))

        case agg @ Aggregate(groupExps, aggExps,
        j @ Join(_, _, Inner | LeftOuter | RightOuter, condition, _))
          if groupExps.forall(_.isInstanceOf[Attribute]) &&
            aggExps.forall(e => e.deterministic && isSimpleExpression(e)) &&
            agg.collectAggregateExprs.forall(e => pushableAggExp(e) || pushableCountExp(e)) =>
          pushAggThroughJoin(agg, j.output, j)

        case agg @ Aggregate(groupExps, aggExps, Project(projectList,
        j @ Join(_, _, Inner | LeftOuter | RightOuter, _, _)))
          if groupExps.forall(_.isInstanceOf[Attribute]) &&
            aggExps.forall(e => e.deterministic && isSimpleExpression(e)) &&
            projectList.forall(_.deterministic) &&
            agg.collectAggregateExprs.forall(e => pushableAggExp(e) || pushableCountExp(e)) =>
          pushAggThroughJoin(agg, projectList, j)
      }
    }
  }
}
