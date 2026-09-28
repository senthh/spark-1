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
 * Pushes a partial (pre-)aggregate from a group-by below a [[Join]].
 *
 * Mirrors the paper's Fig 9, restricted to the conservative, provably-correct shape that
 * dominates the TPC-DS star pattern: the grouping keys and every aggregate-function argument come
 * from the '''left''' input of an inner-style join, and the join sits directly under the aggregate
 * (or under a chain of column-pruning [[Project]]s feeding it).
 *
 *   Aggregate(g, [F(x)])                     Aggregate(g, [merge F])
 *        |                                         |
 *     Join(L, R, on L.k=R.k) (Inner)    ->     Project(extended w/ buffers)
 *      /          \                                 |
 *     L            R                  PartialAgg(g + Lk, [partial F])   R
 *                                            |
 *                                            L        <- fact rows collapsed
 *
 * Why it is correct (the crucial subtlety):
 *   - A partial keyed on ''g alone'' would be wrong: a group whose rows span multiple join-key
 *     values would be collapsed to one row, dropping the multiplicity each (g,k) pair needs for
 *     the join fan-out. So the pushed partial must key on '''g + the left join
 *     keys''', preserving
 *     every distinct (g, k) tuple the join needs.
 *   - The join then fans each partial (g,k) row out to its right-side matches exactly as before.
 *   - A merge Aggregate on top regroups by g (dropping the now-spent join keys k) and re-combines
 *     the partial buffers. Because the merge functions are associative, the total is identical to
 *     aggregating the raw left input through the join.
 *
 * Scope (conservative): only fires when every grouping expression and every aggregate argument is
 * resolvable to the join's left (fact) output, the join is Inner-like, and there are no non-equi
 * join predicates referencing left columns the partial would not carry. Anything else is left
 * untouched - a partial is always optional, so skipping never changes correctness.
 */
object PushPartialAggregateThroughJoin extends Rule[LogicalPlan] with Logging with PredicateHelper {

  private val ENABLED = SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED
  import PartialAggregatePushDownHelper._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAnyPattern(AGGREGATE, JOIN), ruleId) {

    case agg @ Aggregate(_, _, child, _)
        if SQLConf.get.getConf(ENABLED) &&
          allPlainAttributeGrouping(agg) &&
          // Do not re-fire once we have pushed a partial into the join in a prior iteration.
          !child.exists(_.isInstanceOf[PartialAggregate]) =>
      peelToJoin(agg, child) match {
        case Some(join) if isInnerLike(join.joinType) && aggOnLeftOnly(agg, join) =>
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

  /** True if every grouping key and every aggregate argument resolves to the left output. */
  private def aggOnLeftOnly(agg: Aggregate, join: Join): Boolean = {
    val leftIds = join.left.outputSet.map(_.exprId).toSet
    agg.groupingExpressions.forall(g =>
      g.references.nonEmpty && g.references.forall(r => leftIds.contains(r.exprId))) &&
      distinctAggregateExpressions(agg).forall(ae =>
        ae.references.forall(r => leftIds.contains(r.exprId)))
  }

  /** Builds the pushed-down plan, or returns the original unchanged when not profitable. */
  private def constructPartialBelowJoin(agg: Aggregate, join: Join): LogicalPlan = {
    val grouping = agg.groupingExpressions.map(_.asInstanceOf[Attribute])
    val aggExprs = distinctAggregateExpressions(agg)
    if (aggExprs.isEmpty || !canPreAggregate(aggExprs)) {
      agg
    } else if (!partialCoversConditionRefs(grouping, join)) {
      // The join condition references left columns (e.g. a non-equi predicate) that the partial
      // output would not carry; pushing would orphan them. Skip.
      agg
    } else if (!isProfitableBelowJoin(join, grouping)) {
      agg
    } else {
      pushOntoLeft(agg, join, grouping, aggExprs)
    }
  }

  /**
   * True iff the pushed partial's output set will include every left-side attribute referenced by
   * the join condition (i.e. anything beyond the equi-join keys and grouping keys is not allowed,
   * since the partial output only carries those plus the aggregate buffers).
   */
  private def partialCoversConditionRefs(grouping: Seq[Attribute], join: Join): Boolean = {
    val leftIds = join.left.outputSet.map(_.exprId).toSet
    val groupingIds = grouping.map(_.exprId).toSet
    val joinKeyIds =
      equiJoinKeysOnLeft(join, leftIds).collect { case a: Attribute => a.exprId }.toSet
    val covered = groupingIds ++ joinKeyIds
    join.condition.map(splitConjunctivePredicates).getOrElse(Nil).forall { pred =>
      pred.references.forall(r =>
        !leftIds.contains(r.exprId) || covered.contains(r.exprId))
    }
  }

  /**
   * Pushes a partial aggregate onto the left (fact) input, preserving the join keys in the
   * partial's grouping so the join fan-out is unchanged (see class docs).
   */
  private def pushOntoLeft(
      agg: Aggregate,
      join: Join,
      grouping: Seq[Attribute],
      aggExprs: Seq[AggregateExpression]): LogicalPlan = {
    val leftIds = join.left.outputSet.map(_.exprId).toSet

    // Left equi-join keys (as attributes resolvable to the left input) - added to the grouping so
    // multiplicity is preserved through the join.
    val leftJoinKeys = equiJoinKeysOnLeft(join, leftIds)

    // Dedup keys keeping order: grouping first, then any join key not already a grouping key.
    val partialGrouping = (grouping ++ leftJoinKeys.map(_.asInstanceOf[Attribute])).distinct
    // The merge re-groups on the original grouping keys only (join keys are spent by the join).
    val mergeGrouping = grouping

    val partialAggExprs = aggExprs.map(_.copy(mode = Partial))
    val partial = PartialAggregate(partialGrouping, partialAggExprs, join.left)
    val newJoin = join.copy(left = partial)

    // The pass-through Project(s) column-pruning placed between the aggregate and the join
    // reference the raw left columns that the partial now absorbs. They only trimmed columns (bare
    // attributes, unchanged ids), so they are redundant once the partial carries grouping keys +
    // buffer columns - which is exactly what the merge Aggregate reads. Root the merge over the
    // join directly; its output schema is unchanged (mergeResultExpressions preserves the original
    // result aliases/ids), so nodes above are not affected.
    val mergeResultExprs =
      PartialAggregatePushDownHelper.mergeResultExpressions(
        partialAggExprs, agg.aggregateExpressions)

    Aggregate(mergeGrouping, mergeResultExprs, newJoin)
  }

  /** Left-side equi-join keys, as expressions resolved to the left input. */
  private def equiJoinKeysOnLeft(
      join: Join,
      leftIds: Set[org.apache.spark.sql.catalyst.expressions.ExprId]): Seq[Expression] = {
    join.condition.map(splitConjunctivePredicates).getOrElse(Nil).flatMap {
      case EqualTo(l, r)
          if l.references.size == 1 && r.references.size == 1 =>
        if (leftIds.contains(l.references.head.exprId)) Some(l)
        else if (leftIds.contains(r.references.head.exprId)) Some(r)
        else None
      case _ => None
    }
  }

  /**
   * Cost gate. Keeps the partial (conservatively) when stats are unavailable; otherwise requires
   * the estimated partial-group count (grouping + join keys, since that is what it actually groups
   * on) to drop below the reduction threshold.
   */
  private def isProfitableBelowJoin(
      join: Join,
      grouping: Seq[Attribute]): Boolean = {
    val threshold = SQLConf.get.getConf(SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_THRESHOLD)
    if (threshold >= 1.0) {
      true
    } else {
      val groupingIds = grouping.map(_.exprId).toSet
      val extraKeys = equiJoinKeysOnLeft(join, join.left.outputSet.map(_.exprId).toSet)
        .collect { case a: Attribute if !groupingIds.contains(a.exprId) => a }
      val effectiveGrouping = grouping ++ extraKeys

      join.left.stats.rowCount.map { childRows =>
        val groupingStats = effectiveGrouping.flatMap { a =>
          join.left.stats.attributeStats.get(a).flatMap(_.distinctCount)
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
