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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Partial}
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate, Join, LogicalPlan, PartialAggregate, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, JOIN}
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes partial (pre-)aggregates from a group-by below a [[Join]].
 *
 * Mirrors the paper's Fig 9: grouping keys and aggregate-function arguments are split across the
 * two join inputs, and a partial aggregate is pushed onto '''each''' input that contributes them.
 * The classical star shape (aggregate grouped by a dimension attribute) falls out as the special
 * case where grouping + aggregates live on the right (dimension) side and the fact side only
 * contributes its join key.
 *
 *   Aggregate(g, [F(x)])                    Aggregate(g, [merge F_L, merge F_R])
 *        |                                          |
 *     Join(L, R, on L.k=R.k) (Inner)  ->       Join(
 *      /          \                              PartialAgg(g_L + Lk, [partial F_L])  <- on L
 *     L            R                            PartialAgg(g_R + Rk, [partial F_R])  <- on R
 *
 * Why it is correct (the crucial subtlety):
 *   - A partial keyed on a side's grouping keys ''alone'' is wrong: a group whose rows span
 *     multiple join-key values would be collapsed to one row, dropping the multiplicity each
 *     (side-key, join-key) tuple needs for the join fan-out. So each pushed partial must key on
 *     '''its side's grouping keys + that side's join keys''', preserving every distinct tuple the
 *     join needs.
 *   - The join then fans each partial row out to its matches on the other side exactly as before.
 *   - A merge Aggregate on top regroups by the full grouping key set (dropping the now-spent join
 *     keys) and associatively re-combines the partial buffers.
 *
 * Scope (conservative): inner-style joins only; each grouping key and each aggregate function must
 * resolve entirely to one input (an aggregate spanning both inputs is rejected); and the join
 * condition must not reference, on a given side, columns that the pushed partial would not carry.
 * Anything else is left untouched - a partial is always optional, so skipping never changes
 * correctness.
 */
object PushPartialAggregateThroughJoin extends Rule[LogicalPlan] with Logging with PredicateHelper {

  private val ENABLED = SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED

  /** (group keys, agg functions, join keys) for one side of a split aggregate-over-join. */
  private case class Side(groups: Seq[Attribute], aggs: Seq[AggregateExpression],
      joinKeys: Seq[Attribute])

  import PartialAggregatePushDownHelper._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAnyPattern(AGGREGATE, JOIN), ruleId) {

    case agg @ Aggregate(_, _, child, _)
        if SQLConf.get.getConf(ENABLED) &&
          allPlainAttributeGrouping(agg) &&
          // Do not re-fire once we have pushed a partial into the join in a prior iteration.
          !child.exists(_.isInstanceOf[PartialAggregate]) =>
      peelToJoin(agg, child) match {
        case Some(join) if isInnerLike(join.joinType) =>
          constructPartialBelowJoin(agg, join)
        case _ => agg
      }
  }

  private def isInnerLike(t: org.apache.spark.sql.catalyst.plans.JoinType): Boolean =
    t.isInstanceOf[InnerLike]

  /**
   * Peels a chain of pass-through [[Project]]s above a join, returning the join. A project is
   * pass-through here if every one of its expressions is an attribute-alias, which is exactly what
   * column pruning produces between an aggregate and its join.
   */
  private def peelToJoin(agg: Aggregate, child: LogicalPlan): Option[Join] = child match {
    case join: Join => Some(join)
    case Project(projectList, projChild) if projectList.forall(_.isInstanceOf[Attribute]) =>
      peelToJoin(agg, projChild)
    case _ => None
  }

  /** Builds the pushed-down plan, or returns the original unchanged when not suitable. */
  private def constructPartialBelowJoin(agg: Aggregate, join: Join): LogicalPlan = {
    val aggExprs = distinctAggregateExpressions(agg)
    if (aggExprs.isEmpty || !canPreAggregate(aggExprs)) {
      agg
    } else {
      splitAcrossSides(agg, join, aggExprs) match {
        case Some((left, right))
            if conditionRefsCarried(left, right, join) &&
              (left.groups.nonEmpty || right.groups.nonEmpty) &&
              // Correctness: we collapse exactly one side (the one carrying the aggregates); this
              // is only sound when the OTHER side has no aggregates of its own - if both
              // sides had a measure, aggregating the raw side's measure over the collapsed
              // join would multiply it by the collapse ratio. Restrict to one side only.
              (left.aggs.isEmpty || right.aggs.isEmpty) &&
              isProfitable(left, right, join) =>
          pushBothSides(agg, join, left, right)
        case _ => agg
      }
    }
  }

  /**
   * Splits the aggregate's grouping keys and functions across the two join inputs. Returns None
   * unless every grouping key and every aggregate function resolves entirely to exactly one input.
   */
  private def splitAcrossSides(
      agg: Aggregate,
      join: Join,
      aggExprs: Seq[AggregateExpression]): Option[(Side, Side)] = {
    val lIds = join.left.outputSet.map(_.exprId).toSet
    val rIds = join.right.outputSet.map(_.exprId).toSet

    val lKeys = scala.collection.mutable.ArrayBuffer[Attribute]()
    val rKeys = scala.collection.mutable.ArrayBuffer[Attribute]()
    for (g <- agg.groupingExpressions.map(_.asInstanceOf[Attribute])) {
      if (lIds.contains(g.exprId)) lKeys += g
      else if (rIds.contains(g.exprId)) rKeys += g
      else return None // grouping key resolves to neither input - cannot split
    }

    val lAggs = scala.collection.mutable.ArrayBuffer[AggregateExpression]()
    val rAggs = scala.collection.mutable.ArrayBuffer[AggregateExpression]()
    for (ae <- aggExprs) {
      val refs = ae.references
      if (refs.forall(r => lIds.contains(r.exprId))) lAggs += ae
      else if (refs.forall(r => rIds.contains(r.exprId))) rAggs += ae
      else return None // agg spans both inputs - cannot push a per-side partial
    }

    val lJoinKeys = equiJoinKeysOnSide(join, lIds).collect { case a: Attribute => a }
    val rJoinKeys = equiJoinKeysOnSide(join, rIds).collect { case a: Attribute => a }

    Some(Side(lKeys.toSeq, lAggs.toSeq, lJoinKeys) -> Side(rKeys.toSeq, rAggs.toSeq, rJoinKeys))
  }

  /**
   * True iff the pushed partials will carry every attribute the join condition references on each
   * side (a side carries its grouping keys, its join keys and its aggregate-buffer columns).
   */
  private def conditionRefsCarried(left: Side, right: Side, join: Join): Boolean = {
    val lIds = join.left.outputSet.map(_.exprId).toSet
    val rIds = join.right.outputSet.map(_.exprId).toSet
    val lCarried = (left.groups ++ left.joinKeys).map(_.exprId).toSet
    val rCarried = (right.groups ++ right.joinKeys).map(_.exprId).toSet
    join.condition.map(splitConjunctivePredicates).getOrElse(Nil).forall { pred =>
      pred.references.forall { spRef =>
        if (lIds.contains(spRef.exprId)) lCarried.contains(spRef.exprId)
        else if (rIds.contains(spRef.exprId)) rCarried.contains(spRef.exprId)
        else true
      }
    }
  }

  /**
   * Pushes a partial aggregate onto the side that carries the aggregate functions (the "fact"
   * side) and leaves the other side raw. Collapsing both sides is unsafe: an inner join's fan-out
   * multiplicity comes from the raw side's duplicate keys, so if both sides collapse, that
   * multiplicity is destroyed. Collapsing one side is always safe, because every row merged on it
   * shares its join key and therefore matches the same raw-side rows - so Sum/Merge sees the same
   * total (sum of merged values) times the same fan-out count. The merge Aggregate regroups by the
   * full grouping key set (any grouping keys that live on the raw dimension side are still
   * present there) and reads the partial buffers.
   */
  private def pushBothSides(agg: Aggregate, join: Join, left: Side, right: Side): LogicalPlan = {
    // Collapse the side with aggregates; keep the other raw (it carries the fan-out multiplicity).
    val collapseLeft = left.aggs.nonEmpty

    val lPartialKey = dedup(left.groups ++ left.joinKeys)
    val rPartialKey = dedup(right.groups ++ right.joinKeys)
    val lPartialAggs = left.aggs.map(_.copy(mode = Partial))
    val rPartialAggs = right.aggs.map(_.copy(mode = Partial))

    val newLeft = if (collapseLeft) {
      PartialAggregate(lPartialKey, lPartialAggs, join.left)
    } else {
      join.left
    }
    val newRight = if (collapseLeft) {
      join.right
    } else {
      PartialAggregate(rPartialKey, rPartialAggs, join.right)
    }

    val newJoin = join.copy(left = newLeft, right = newRight)
    val mergeGrouping = agg.groupingExpressions.asInstanceOf[Seq[Attribute]]

    // The partial buffers from the collapsed side are referenced by resultId so the merge reads
    // them; aggregates on the raw side are unchanged (re-aggregate at the merge over the join).
    val allPartialAggs = if (collapseLeft) lPartialAggs else rPartialAggs
    val mergeResultExprs =
      PartialAggregatePushDownHelper.mergeResultExpressions(
        allPartialAggs, agg.aggregateExpressions)

    Aggregate(mergeGrouping, mergeResultExprs, newJoin)
  }

  private def dedup(attrs: Seq[Attribute]): Seq[Attribute] = {
    val seen = scala.collection.mutable.LinkedHashSet[
      org.apache.spark.sql.catalyst.expressions.ExprId]()
    attrs.filter(a => seen.add(a.exprId))
  }

  /**
   * Cost gate. Keeps the push-down (conservatively) when stats are unavailable; otherwise requires
   * the estimated partial-group count on a side that actually aggregates (i.e. has grouping keys)
   * to drop below the reduction threshold. A side with no grouping keys only contributes its join
   * key (a low-cardinality fan filter) and is not itself the "collapse" opportunity.
   */
  private def isProfitable(left: Side, right: Side, join: Join): Boolean = {
    val threshold = SQLConf.get.getConf(SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_THRESHOLD)
    if (threshold >= 1.0) {
      true
    } else {
      List(
        (left, join.left),
        (right, join.right)
      ).filter { case (side, _) => side.groups.nonEmpty }.exists {
        case (side, plan) =>
          val effectiveGrouping = dedup(side.groups ++ side.joinKeys)
          plan.stats.rowCount.map { childRows =>
            val groupingStats = effectiveGrouping.flatMap { a =>
              plan.stats.attributeStats.get(a).flatMap(_.distinctCount)
            }
            if (groupingStats.size != effectiveGrouping.length) {
              true // cannot estimate - keep the partial (merge still yields the correct result)
            } else {
              val estimatedGroups: BigInt = groupingStats.foldLeft(BigInt(1))(_ * _)
              estimatedGroups.toDouble / childRows.toDouble <= threshold
            }
          }.getOrElse(true)
      }
    }
  }

  /** Side-specific equi-join keys, as expressions resolved to that side's input. */
  private def equiJoinKeysOnSide(
      join: Join,
      sideIds: Set[org.apache.spark.sql.catalyst.expressions.ExprId]): Seq[Expression] = {
    join.condition.map(splitConjunctivePredicates).getOrElse(Nil).flatMap {
      case EqualTo(l, r)
          if l.references.size == 1 && r.references.size == 1 =>
        if (sideIds.contains(l.references.head.exprId)) Some(l)
        else if (sideIds.contains(r.references.head.exprId)) Some(r)
        else None
      case _ => None
    }
  }
}
