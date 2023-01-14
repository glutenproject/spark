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

package org.apache.spark.sql.execution.window

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, DenseRank, Expression, Rank, RowNumber, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

sealed trait WindowGroupLimitMode

case object Partial extends WindowGroupLimitMode

case object Final extends WindowGroupLimitMode

/**
 * This operator is designed to filter out unnecessary rows before WindowExec
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]]
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]]
 * @param rankLikeFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 */
case class WindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = mode match {
    case Partial => Seq(orderSpec)
    case Final => Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = mode match {
    case Partial =>
      rankLikeFunction match {
        case _: RowNumber if partitionSpec.isEmpty =>
          child.execute().mapPartitionsInternal(new SimpleIterator(output, _, limit))
        case _: RowNumber =>
          child.execute().mapPartitionsInternal(
            SimpleHashTableIterator(partitionSpec, output, _, limit))
        case _: Rank if partitionSpec.isEmpty =>
          child.execute().mapPartitionsInternal(new RankIterator(output, _, orderSpec, limit))
        case _: Rank =>
          child.execute().mapPartitionsInternal(
            RankHashTableIterator(partitionSpec, output, _, orderSpec, limit))
        case _: DenseRank if partitionSpec.isEmpty =>
          child.execute().mapPartitionsInternal(new DenseRankIterator(output, _, orderSpec, limit))
        case _: DenseRank =>
          child.execute().mapPartitionsInternal(
            DenseRankHashTableIterator(partitionSpec, output, _, orderSpec, limit))
      }
    case Final =>
      rankLikeFunction match {
        case _: RowNumber =>
          child.execute().mapPartitionsInternal(
            SimpleGroupLimitIterator(partitionSpec, output, _, limit))
        case _: Rank =>
          child.execute().mapPartitionsInternal(
            RankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
        case _: DenseRank =>
          child.execute().mapPartitionsInternal(
            DenseRankGroupLimitIterator(partitionSpec, output, _, orderSpec, limit))
      }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowGroupLimitExec =
    copy(child = newChild)
}

trait BaseIterator extends Iterator[InternalRow] {

  def output: Seq[Attribute]

  def input: Iterator[InternalRow]

  def limit: Int

  var rank = 0

  var nextRow: UnsafeRow = null

  // Increase the rank value.
  def increaseRank(): Unit

  override def hasNext: Boolean =
    rank < limit && input.hasNext

  override def next(): InternalRow = {
    nextRow = input.next().asInstanceOf[UnsafeRow]
    increaseRank()
    nextRow
  }
}

class SimpleIterator(
    val output: Seq[Attribute],
    val input: Iterator[InternalRow],
    val limit: Int) extends BaseIterator {

  override def increaseRank(): Unit = {
    rank += 1
  }
}

trait OrderSpecProvider {
  def output: Seq[Attribute]
  def orderSpec: Seq[SortOrder]
  val ordering = GenerateOrdering.generate(orderSpec, output)
}

class RankIterator(
    val output: Seq[Attribute],
    val input: Iterator[InternalRow],
    val orderSpec: Seq[SortOrder],
    val limit: Int) extends BaseIterator with OrderSpecProvider {

  var count = 0
  var currentRank: UnsafeRow = null

  override def increaseRank(): Unit = {
    if (count == 0) {
      currentRank = nextRow.copy()
    } else {
      if (ordering.compare(currentRank, nextRow) != 0) {
        rank = count
        currentRank = nextRow.copy()
      }
    }
    count += 1
  }
}

class DenseRankIterator(
    val output: Seq[Attribute],
    val input: Iterator[InternalRow],
    val orderSpec: Seq[SortOrder],
    val limit: Int) extends BaseIterator with OrderSpecProvider {

  var currentRank: UnsafeRow = null

  override def increaseRank(): Unit = {
    if (currentRank == null) {
      currentRank = nextRow.copy()
    } else {
      if (ordering.compare(currentRank, nextRow) != 0) {
        rank += 1
        currentRank = nextRow.copy()
      }
    }
  }
}

trait PartitionSpecProvider {
  def partitionSpec: Seq[Expression]
  def output: Seq[Attribute]

  protected val grouping = UnsafeProjection.create(partitionSpec, output)
}

case class SimpleHashTableIterator(
    partitionSpec: Seq[Expression],
    override val output: Seq[Attribute],
    override val input: Iterator[InternalRow],
    override val limit: Int)
  extends SimpleIterator(output, input, limit) with PartitionSpecProvider {

  val groupToRank = mutable.HashMap.empty[UnsafeRow, Int]

  override def hasNext: Boolean = input.hasNext

  override def next(): InternalRow = {
    do {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      val nextGroup = grouping(nextRow)
      rank = groupToRank.getOrElse(nextGroup, 0)
      increaseRank()
      groupToRank(nextGroup) = rank
    } while (rank > limit && input.hasNext)

    nextRow
  }
}

 case class RankHashTableIterator(
    partitionSpec: Seq[Expression],
    override val output: Seq[Attribute],
    override val input: Iterator[InternalRow],
    override val orderSpec: Seq[SortOrder],
    override val limit: Int)
  extends RankIterator(output, input, orderSpec, limit) with PartitionSpecProvider {

  val groupToRankInfo = mutable.HashMap.empty[UnsafeRow, (Int, Int, UnsafeRow)]

  override def hasNext: Boolean = input.hasNext

  override def next(): InternalRow = {
    do {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      val nextGroup = grouping(nextRow)
      val rankInfo = groupToRankInfo.getOrElse(nextGroup, (0, 0, null))
      count = rankInfo._1
      rank = rankInfo._2
      currentRank = rankInfo._3
      increaseRank()
      groupToRankInfo(nextGroup) = (count, rank, currentRank)
    } while (rank > limit && input.hasNext)

    nextRow
  }
 }

 case class DenseRankHashTableIterator(
    partitionSpec: Seq[Expression],
    override val output: Seq[Attribute],
    override val input: Iterator[InternalRow],
    override val orderSpec: Seq[SortOrder],
    override val limit: Int)
  extends DenseRankIterator(output, input, orderSpec, limit) with PartitionSpecProvider {

  val groupToRankInfo = mutable.HashMap.empty[UnsafeRow, (Int, UnsafeRow)]

  override def hasNext: Boolean = input.hasNext

  override def next(): InternalRow = {
    do {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      val nextGroup = grouping(nextRow)
      val rankInfo = groupToRankInfo.getOrElse(nextGroup, (0, null))
      rank = rankInfo._1
      currentRank = rankInfo._2
      increaseRank()
      groupToRankInfo(nextGroup) = (rank, currentRank)
    } while (rank > limit && input.hasNext)

    nextRow
  }
 }

abstract class WindowIterator extends Iterator[InternalRow] {

  def partitionSpec: Seq[Expression]

  def output: Seq[Attribute]

  def input: Iterator[InternalRow]

  def limit: Int

  val grouping = UnsafeProjection.create(partitionSpec, output)

  // Manage the stream and the grouping.
  var nextRow: UnsafeRow = null
  var nextGroup: UnsafeRow = null
  var nextRowAvailable: Boolean = false
  protected[this] def fetchNextRow(): Unit = {
    nextRowAvailable = input.hasNext
    if (nextRowAvailable) {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      nextGroup = grouping(nextRow)
    } else {
      nextRow = null
      nextGroup = null
    }
  }
  fetchNextRow()

  var rank = 0

  // Increase the rank value.
  def increaseRank(): Unit

  // Clear the rank value.
  def clearRank(): Unit

  var bufferIterator: Iterator[InternalRow] = _

  private[this] def fetchNextGroup(): Unit = {
    clearRank()
    bufferIterator = createGroupIterator()
  }

  override final def hasNext: Boolean =
    (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable

  override final def next(): InternalRow = {
    // Load the next partition if we need to.
    if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
      fetchNextGroup()
    }

    if (bufferIterator.hasNext) {
      bufferIterator.next()
    } else {
      throw new NoSuchElementException
    }
  }

  private def createGroupIterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      // Before we start to fetch new input rows, make a copy of nextGroup.
      val currentGroup = nextGroup.copy()

      def hasNext: Boolean = {
        if (nextRowAvailable) {
          if (rank >= limit && nextGroup == currentGroup) {
            // Skip all the remaining rows in this group
            do {
              fetchNextRow()
            } while (nextRowAvailable && nextGroup == currentGroup)
            false
          } else {
            // Returns true if there are more rows in this group.
            nextGroup == currentGroup
          }
        } else {
          false
        }
      }

      def next(): InternalRow = {
        val currentRow = nextRow.copy()
        increaseRank()
        fetchNextRow()
        currentRow
      }
    }
  }
}

case class SimpleGroupLimitIterator(
    partitionSpec: Seq[Expression],
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    limit: Int) extends WindowIterator {

  override def increaseRank(): Unit = {
    rank += 1
  }

  override def clearRank(): Unit = {
    rank = 0
  }
}

case class RankGroupLimitIterator(
    partitionSpec: Seq[Expression],
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int) extends WindowIterator {
  val ordering = GenerateOrdering.generate(orderSpec, output)
  var count = 0
  var currentRank: UnsafeRow = null

  override def increaseRank(): Unit = {
    if (count == 0) {
      currentRank = nextRow.copy()
    } else {
      if (ordering.compare(currentRank, nextRow) != 0) {
        rank = count
        currentRank = nextRow.copy()
      }
    }
    count += 1
  }

  override def clearRank(): Unit = {
    count = 0
    rank = 0
    currentRank = null
  }
}

case class DenseRankGroupLimitIterator(
    partitionSpec: Seq[Expression],
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int) extends WindowIterator {
  val ordering = GenerateOrdering.generate(orderSpec, output)
  var currentRank: UnsafeRow = null

  override def increaseRank(): Unit = {
    if (currentRank == null) {
      currentRank = nextRow.copy()
    } else {
      if (ordering.compare(currentRank, nextRow) != 0) {
        rank += 1
        currentRank = nextRow.copy()
      }
    }
  }

  override def clearRank(): Unit = {
    rank = 0
    currentRank = null
  }
}
