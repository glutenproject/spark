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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LeafNode, LogicalPlan, Project, SerializeFromObject}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, JOIN}

/**
 * This rule eliminates the [[Join]] if all the join side are [[Aggregate]]s by combine these
 * [[Aggregate]]s. This rule also support the nested [[Join]], as long as all the join sides for
 * every [[Join]] are [[Aggregate]]s.
 *
 * Note: The [[Aggregate]]s to be merged must not exists filter.
 */
object EliminateJoinByCombineAggregate extends Rule[LogicalPlan] {

  private def isSupportedJoinType(joinType: JoinType): Boolean =
    Seq(Inner, Cross, LeftOuter, RightOuter, FullOuter).contains(joinType)

  // Collect all the Aggregates from both side of single or nested Join.
  private def collectAggregate(plan: LogicalPlan, aggregates: ArrayBuffer[Aggregate]): Boolean = {
    var flag = true
    if (plan.containsAnyPattern(JOIN, AGGREGATE)) {
      plan match {
        case Join(left: Aggregate, right: Aggregate, _, None, _)
          if left.groupingExpressions.isEmpty && right.groupingExpressions.isEmpty &&
            left.aggregateExpressions.forall(filterNotDefined) &&
            right.aggregateExpressions.forall(filterNotDefined) =>
          aggregates += left
          aggregates += right
        case Join(left @ Join(_, _, joinType, None, _), right: Aggregate, _, None, _)
          if isSupportedJoinType(joinType) && right.groupingExpressions.isEmpty &&
            right.aggregateExpressions.forall(filterNotDefined) =>
          flag = collectAggregate(left, aggregates)
          aggregates += right
        case Join(left: Aggregate, right @ Join(_, _, joinType, None, _), _, None, _)
          if isSupportedJoinType(joinType) && left.groupingExpressions.isEmpty &&
            left.aggregateExpressions.forall(filterNotDefined) =>
          aggregates += left
          flag = collectAggregate(right, aggregates)
        // The side of Join is neither Aggregate nor Join.
        case _ => flag = false
      }
    }

    flag
  }

  // TODO Support aggregate expression with filter clause.
  private def filterNotDefined(ne: NamedExpression): Boolean = {
    ne match {
      case Alias(ae: AggregateExpression, _) => ae.filter.isEmpty
      case ae: AggregateExpression => ae.filter.isEmpty
    }
  }

  // Merge the multiple Aggregates.
  private def mergePlan(
      left: LogicalPlan,
      right: LogicalPlan): Option[(LogicalPlan, Map[Expression, Attribute], Seq[Expression])] = {
    (left, right) match {
      case (la: Aggregate, ra: Aggregate) =>
        val mergedChildPlan = mergePlan(la.child, ra.child)
        mergedChildPlan.map { case (newChild, outputMap, filters) =>
          val rightAggregateExprs = ra.aggregateExpressions.map { ne =>
            ne.transform {
              case attr: Attribute =>
                outputMap.getOrElse(attr.canonicalized, attr)
            }.asInstanceOf[NamedExpression]
          }

          val mergedAggregateExprs = if (filters.length == 2) {
            la.aggregateExpressions.map { ne =>
              ne.transform {
                case ae @ AggregateExpression(_, _, _, None, _) =>
                  ae.copy(filter = Some(filters.head))
              }.asInstanceOf[NamedExpression]
            } ++ rightAggregateExprs.map { ne =>
              ne.transform {
                case ae @ AggregateExpression(_, _, _, None, _) =>
                  ae.copy(filter = Some(filters.last))
              }.asInstanceOf[NamedExpression]
            }
          } else {
            la.aggregateExpressions ++ rightAggregateExprs
          }

          (Aggregate(Seq.empty, mergedAggregateExprs, newChild), Map.empty, Seq.empty)
        }
      case (lp: Project, rp: Project) =>
        val mergedInfo = mergePlan(lp.child, rp.child)
        val mergedProjectList = ArrayBuffer[NamedExpression](lp.projectList: _*)

        mergedInfo.map { case (newChild, outputMap, filters) =>
          val allFilterReferences = filters.flatMap(_.references)
          val newOutputMap = (rp.projectList ++ allFilterReferences).map { ne =>
            val mapped = ne.transform {
              case attr: Attribute =>
                outputMap.getOrElse(attr.canonicalized, attr)
            }.asInstanceOf[NamedExpression]

            val withoutAlias = mapped match {
              case Alias(child, _) => child
              case e => e
            }

            val outputAttr = mergedProjectList.find {
              case Alias(child, _) => child semanticEquals withoutAlias
              case e => e semanticEquals withoutAlias
            }.getOrElse {
              mergedProjectList += mapped
              mapped
            }.toAttribute
            ne.toAttribute.canonicalized -> outputAttr
          }.toMap

          (Project(mergedProjectList.toSeq, newChild), newOutputMap, filters)
        }
      // TODO support only one side contains Filter
      case (lf: Filter, rf: Filter) =>
        val mergedInfo = mergePlan(lf.child, rf.child)
        mergedInfo.map { case (newChild, outputMap, _) =>
          val rightCondition = rf.condition transform {
            case attr: Attribute =>
              outputMap.getOrElse(attr.canonicalized, attr)
          }
          val newCondition = Or(lf.condition, rightCondition)

          (Filter(newCondition, newChild), outputMap, Seq(lf.condition, rightCondition))
        }
      case (ll: LeafNode, rl: LeafNode) =>
        if (ll.canonicalized == rl.canonicalized) {
          val outputMap = rl.output.zip(ll.output).map { case (ra, la) =>
            ra.canonicalized -> la
          }.toMap

          Some((ll, outputMap, Seq.empty))
        } else {
          None
        }
      case (ls: SerializeFromObject, rs: SerializeFromObject) =>
        if (ls.canonicalized == rs.canonicalized) {
          val outputMap = rs.output.zip(ls.output).map { case (ra, la) =>
            ra.canonicalized -> la
          }.toMap

          Some((ls, outputMap, Seq.empty))
        } else {
          None
        }
      case _ => None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.eliminateJoinByCombineAggregateEnabled) return plan

    plan.transformDownWithPruning(_.containsAnyPattern(JOIN, AGGREGATE), ruleId) {
      case j @ Join(_, _, joinType, None, _) if isSupportedJoinType(joinType) =>
        val aggregates = ArrayBuffer.empty[Aggregate]
        if (collectAggregate(j, aggregates)) {
          var finalAggregate: Option[LogicalPlan] = None
          for ((aggregate, i) <- aggregates.tail.zipWithIndex
               if i == 0 || finalAggregate.isDefined) {
            val mergedAggregate = mergePlan(finalAggregate.getOrElse(aggregates.head), aggregate)
            finalAggregate = mergedAggregate.map(_._1)
          }

          finalAggregate.getOrElse(j)
        } else {
          j
        }
    }
  }
}