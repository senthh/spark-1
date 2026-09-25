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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Count, Max, Min, Partial, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, PartialAggregate, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, UNION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Gives first-class support to partial (pre-)aggregation in the Catalyst logical optimizer.
 *
 * The rule seeds an optional [[PartialAggregate]] from a group-by ([[Aggregate]]) and pushes it
 * down through operators that are safe to re-group, currently `Union` (the TPCDS Q11-class win):
 *
 *   Aggregate(g, [F(x)])                   Aggregate(g, [F(buf_F)])     <- normal Complete merge
 *        |                                     |
 *       Union              ->                 Union
 *      /     \                              /        \
 *     A       B                 PartialAgg(g, A)    PartialAgg(g, B)
 *
 * Semantics: each union input is pre-collapsed to one row per (g, ...) group by computing the
 * partial results of the aggregate functions. The union then concatenates the collapsed groups
 * and the top '''merge aggregate''' - a perfectly normal group-by - re-aggregates the partial
 * results, reading the aggregate buffer columns the partials emitted instead of the original
 * source columns. The result is identical to aggregating the raw inputs.
 *
 * '''Correctness model''' - a partial aggregate is always ''optional'': the merge aggregate above
 * recomputes the fully aggregated value, so dropping every partial never changes a query result.
 * The cost model below only decides whether a partial removes enough rows (reduction ratio under
 * a threshold) to justify its own hash-map build; it can never depend on costing for correctness.
 *
 * '''Buffer alignment (why no ExprId surgery)''' - each union arm is built from the ''same''
 * aggregate function objects, so every arm's `PartialAggregate` emits identical
 * `aggBufferAttributes` (stable attribute references tied to the function). The union therefore
 * aligns its inputs by construction. The merge aggregate redirects each Complete-mode function to
 * read its corresponding buffer column (Sum over the sum-buffer, Min over the min-buffer, etc.).
 * Because the buffer columns are plain columns at the union output, the existing physical planner
 * executes the merge with its normal Partial -> shuffle -> Final pipeline and no changes.
 *
 * '''Scope''' - this first implementation conservatively supports only the aggregates with a
 * clean buffer-to-result mapping: `Sum`, `Min`, `Max`, `Count`. Everything else (Average, distinct,
 * filtered, non-commutative) is left untouched (the partial is simply not introduced).
 */
object PushPartialAggregateOverUnion extends Rule[LogicalPlan] with Logging {

  private val ENABLED = SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED
  private val THRESHOLD = SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_THRESHOLD

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAnyPattern(AGGREGATE, UNION), ruleId) {

    case agg @ Aggregate(_, _, child: Union, _)
        if SQLConf.get.getConf(ENABLED) &&
          // Do not re-fire in the fixed-point batch: once we push partials into a union's arms,
          // the new merge Aggregate above still has a Union child and would otherwise match again,
          // nesting partial aggregates indefinitely. Skip if any arm is already a partial.
          !child.children.exists(_.isInstanceOf[PartialAggregate]) =>
      constructPartial(agg)
  }

  /**
   * Rewrites the group-by if (a) it is pre-aggregatable and (b) the partial is estimated to reduce
   * the merged row count by a useful amount. Otherwise returns the original plan unchanged.
   */
  private def constructPartial(agg: Aggregate): LogicalPlan = {
    val groupingExprs = agg.groupingExpressions
    if (groupingExprs.isEmpty) {
      // Global aggregation: the partial would still collapse rows but for a single output row the
      // existing physical aggregation already does this best; skip.
      agg
    } else if (!groupingExprs.forall(_.isInstanceOf[Attribute])) {
      // Only plain-attribute grouping keys are safe to rebind on the merge aggregate. Computed
      // grouping expressions (e.g. substr(c, 1, 2)) would fail to bind after the partial replaces
      // the raw input columns; conservatively skip.
      agg
    } else {
      distinctAggregateExpressions(agg) match {
        case aggExprs if aggExprs.nonEmpty && canPreAggregate(aggExprs)
            && isProfitable(agg, aggExprs) =>
          seedAndMerge(agg, aggExprs)
        case _ => agg
      }
    }
  }

  /**
   * Builds: Aggregate(g, [merge f1, merge f2, ...]) over Union(PartialAgg(g,A), PartialAgg(g,B)).
   */
  private def seedAndMerge(
      agg: Aggregate,
      aggExprs: Seq[AggregateExpression]): LogicalPlan = {
    val groupingExpressions = agg.groupingExpressions
    // Partial-mode functions (identical objects across arms => identical buffer attrs).
    val partialAggExprs = aggExprs.map(_.copy(mode = Partial))

    val union = agg.child.asInstanceOf[Union]
    // Guarded: every grouping key is an Attribute referencing the union output.
    val groupAttrs = groupingExpressions.map(_.asInstanceOf[Attribute])
    // Merge re-groups on the original group-by keys (union-canonical ids).
    val groupingAttrs = groupAttrs

    // Every partial aggregate must reference the same (union-canonical) column ids so that the
    // buffer attributes it emits align for the merge. But each arm carries its own ids. So we
    // wrap each arm in a pass-through Project that renames its columns to the union output ids
    // (positionally), mirroring what Union.mergeChildOutputs does internally. The same partial
    // then binds to every (renamed) arm, and its buffer columns match across arms by construction.
    val unionOutput = union.output
    def canonicalize(arm: LogicalPlan): LogicalPlan = {
      val mapping = arm.output.zip(unionOutput)
      if (mapping.forall { case (a, u) => a.exprId == u.exprId }) {
        arm
      } else {
        val aliases = mapping.map { case (a, u) =>
          Alias(a, u.name)(exprId = u.exprId)
        }
        Project(aliases, arm)
      }
    }
    def makePartial(arm: LogicalPlan): LogicalPlan =
      PartialAggregate(groupingAttrs, partialAggExprs, canonicalize(arm))

    val newUnion = union.copy(children = union.children.map(makePartial))

    // Merge result expressions: same grouping columns, but each aggregate function now reads the
    // buffer column emitted by the partial (stable aggBufferAttributes).
    val mergeResultExprs = agg.aggregateExpressions.map { expr =>
      expr.transformDown {
        case ae: AggregateExpression =>
          val mergeFunc = mergeFunction(ae)
          // Preserve the original aggregate's resultId so the output columns above are unchanged.
          AggregateExpression(
            mergeFunc, ae.mode, isDistinct = false, filter = None, resultId = ae.resultId)
      }.asInstanceOf[NamedExpression]
    }

    Aggregate(groupingAttrs, mergeResultExprs, newUnion)
  }

  /**
   * The merge function that reads the partial's aggregate-buffer column for the given function.
   */
  private def mergeFunction(ae: AggregateExpression): AggregateFunction = {
    val bufferAttr = ae.aggregateFunction.aggBufferAttributes
    ae.aggregateFunction match {
      case _: Sum => Sum(bufferAttr.head)
      case _: Count => Sum(bufferAttr.head)
      case _: Min => Min(bufferAttr.head)
      case _: Max => Max(bufferAttr.head)
      case f => throw new IllegalStateException(s"unreachable: $f")
    }
  }

  /** Distinct aggregate expressions referenced by the result expressions, order-preserving. */
  private def distinctAggregateExpressions(agg: Aggregate): Seq[AggregateExpression] = {
    val seen = scala.collection.mutable.LinkedHashMap[AggregateExpression, Unit]()
    agg.aggregateExpressions.foreach { expr =>
      expr.collect { case ae: AggregateExpression => ae }.foreach { ae =>
        seen.getOrElseUpdate(ae, (): Unit)
      }
    }
    seen.keys.toSeq
  }

  private def canPreAggregate(aggs: Seq[AggregateExpression]): Boolean =
    aggs.nonEmpty && aggs.forall { ae =>
      !ae.isDistinct &&
        ae.filter.isEmpty &&
        (ae.aggregateFunction match {
          case _: Sum =>
            // Only safe when the partial buffer is a single column. Decimal / TRY-mode Sum track
            // an `isEmpty`/overflow flag in a second buffer column that a naive `Sum(buffer)` merge
            // would silently drop; exclude those.
            ae.aggregateFunction.aggBufferAttributes.size == 1
          case _: Count | _: Min | _: Max => true
          case _ => false
        })
    }

  /**
   * Keep the partial only if grouping on the aggregate keys is estimated to collapse rows
   * meaningfully (reduction ratio below a threshold). Uses the child's row count and the
   * cardinality of the grouping attributes as a proxy; if statistics are unavailable, keeps
   * the partial (a conservative choice - the merge still yields the correct result).
   */
  private def isProfitable(agg: Aggregate, aggExprs: Seq[AggregateExpression]): Boolean = {
    val threshold = SQLConf.get.getConf(THRESHOLD)
    if (threshold >= 1.0) {
      true
    } else {
      val childStats = agg.child.stats
      childStats.rowCount.map { childRows =>
        val groupingStats = agg.groupingExpressions.flatMap {
          case a: Attribute => childStats.attributeStats.get(a).flatMap(_.distinctCount)
          case _ => None
        }
        if (groupingStats.size != agg.groupingExpressions.length) {
          // Cannot estimate group cardinality; assume it does not reduce enough.
          true
        } else {
          val estimatedGroups: BigInt = groupingStats.foldLeft(BigInt(1))(_ * _)
          val ratio = estimatedGroups.toDouble / childRows.toDouble
          ratio <= threshold
        }
      }.getOrElse(true)
    }
  }
}
