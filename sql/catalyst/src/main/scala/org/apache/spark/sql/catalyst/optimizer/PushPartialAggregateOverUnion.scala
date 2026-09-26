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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, If, Literal, NamedExpression, Not}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Count, Max, Min, Partial, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, PartialAggregate, Project, Range, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, UNION}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

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
          !child.children.exists(_.isInstanceOf[PartialAggregate]) &&
          // A pre-aggregate pays for itself only on real (scan-backed) input. In-memory data
          // (LocalRelation/Range from toDF/range) is collapsed into a single LocalRelation by the
          // optimizer's "LocalRelation" batch (ConvertToLocalRelation, UpdateAttributeNullability).
          // which rebuilds our PartialAggregate's aggregate-expressions and thereby regenerates
          // its aggBufferAttributes exprIds - orphaning the merge references. Skip such arms: the
          // tiny local input makes a partial pointless anyway.
          !isLocallyBacked(child) =>
      constructPartial(agg)
  }

  /** True if any union arm is backed by in-memory (Range/LocalRelation) data. */
  private def isLocallyBacked(union: Union): Boolean = union.children.exists { arm =>
    arm.exists { n =>
      n.isInstanceOf[Range] ||
      n.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LocalRelation]
    }
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
    // Partial-mode functions (identical objects across arms => identical buffer attrs). These are
    // the SAME expression objects the PartialAggregate emits, so rebinding the merge's buffer
    // references by resultId lines up with the union output by construction.
    val partialAggExprs = aggExprs.map(_.copy(mode = Partial))
    val bufferByResultId = partialAggExprs.map(ae => ae.resultId -> ae).toMap

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
    // buffer column emitted by the partial (stable aggBufferAttributes), looked up by resultId so
    // it references exactly the buffer columns the partial emitted.
    val mergeResultExprs = agg.aggregateExpressions.map { expr =>
      expr.transformDown {
        case ae: AggregateExpression =>
          val partial = bufferByResultId(ae.resultId)
          val mergeFunc = mergeFunction(partial)
          // Preserve the original aggregate's resultId so the output columns above are unchanged.
          val merge = AggregateExpression(
            mergeFunc, ae.mode, isDistinct = false, filter = None, resultId = ae.resultId)
          // Re-applying a Complete-mode aggregate to a partial buffer can widen the type
          // (e.g. DECIMAL: summing DECIMAL(22,2) partial sums yields DECIMAL(32,2)). Cast back to
          // the original aggregate's result type so the output schema is unchanged. This is safe:
          // the total of the partial sums fits in the original type by the same argument the
          // original aggregate used.
          if (merge.dataType != ae.dataType) {
            Cast(merge, ae.dataType)
          } else {
            merge
          }
      }.asInstanceOf[NamedExpression]
    }

    Aggregate(groupingAttrs, mergeResultExprs, newUnion)
  }

  /**
   * The merge function that reads the partial's aggregate-buffer column for the given function.
   *
   * For `Sum` with a single-column buffer (Long/integral result) a naive `Sum(buffer)` merge is
   * correct. For `Sum` with a two-column buffer (Decimal / TRY-mode result: `sum` + `isEmpty`),
   * the `isEmpty` flag records whether any non-null input was seen; an all-empty group must merge
   * to NULL, not to the running-zero sum. We therefore null out each empty contribution so the
   * Final `Sum` (which skips NULLs) yields NULL iff every arm was empty, and the correct total
   * otherwise.
   */
  private def mergeFunction(ae: AggregateExpression): AggregateFunction = {
    val bufferAttr = ae.aggregateFunction.aggBufferAttributes
    ae.aggregateFunction match {
      case _: Sum if bufferAttr.size == 2 =>
        val value = bufferAttr(0)
        val isEmpty = bufferAttr(1)
        // If the partial arm saw no non-null input this group, drop its (zero) contribution so the
        // group's merge stays NULL rather than 0. Value column is the running sum.
        Sum(If(Not(isEmpty), value, Literal.create(null, value.dataType)))
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
          case s: Sum =>
            // Single-column buffer (integral/Long result): naive Sum(buffer) merge is fine.
            val buf = s.aggBufferAttributes
            buf.size == 1 ||
              // Decimal Sum tracks `isEmpty` in a second buffer column; supported via the
              // empty-aware merge in [[mergeFunction]]. TRY-mode integral Sum (also 2-column,
              // but with overflow-propagation semantics we do not replicate) stays excluded.
              (buf.size == 2 && s.dataType.isInstanceOf[DecimalType])
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
