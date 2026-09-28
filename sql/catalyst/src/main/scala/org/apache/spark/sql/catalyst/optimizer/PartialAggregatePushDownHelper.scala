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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, If, Literal, Not}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression, AggregateFunction, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DecimalType

/**
 * Shared logic for the partial (pre-)aggregation push-down rules.
 *
 * A partial aggregate is always optional - the final aggregate above re-computes the full result,
 * so dropping every partial never changes a query. These helpers factor the parts of the
 * construction that are common to the Union and Join push-down rules: identifying the distinct
 * aggregate expressions, deciding whether a set is pre-aggregatable, building the merge function
 * that reads a partial's buffer column, and the merge result-expression rewrite.
 */
private[optimizer] object PartialAggregatePushDownHelper extends Logging {

  /** Distinct aggregate expressions referenced by the result expressions, order-preserving. */
  def distinctAggregateExpressions(agg: Aggregate): Seq[AggregateExpression] = {
    val seen = scala.collection.mutable.LinkedHashMap[AggregateExpression, Unit]()
    agg.aggregateExpressions.foreach { expr =>
      expr.collect { case ae: AggregateExpression => ae }.foreach { ae =>
        seen.getOrElseUpdate(ae, (): Unit)
      }
    }
    seen.keys.toSeq
  }

  /**
   * Whether a set of aggregate expressions can be safely pre-aggregated (pushed as a Partial below
   * a join/union and merged above). Only the associative, null-safe, commutative functions with a
   * clean buffer-to-result mapping are supported: `Sum`, `Count`, `Min`, `Max`. Everything else
   * (Average, distinct, filtered, non-commutative) is left untouched.
   */
  def canPreAggregate(aggs: Seq[AggregateExpression]): Boolean =
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
   * The merge function that reads the partial's aggregate-buffer column for the given function.
   *
   * For `Sum` with a single-column buffer (Long/integral result) a naive `Sum(buffer)` merge is
   * correct. For `Sum` with a two-column buffer (Decimal / TRY-mode result: `sum` + `isEmpty`),
   * the `isEmpty` flag records whether any non-null input was seen; an all-empty group must merge
   * to NULL, not to the running-zero sum. We therefore null out each empty contribution so the
   * Final `Sum` (which skips NULLs) yields NULL iff every arm was empty, and the correct total
   * otherwise.
   */
  def mergeFunction(ae: AggregateExpression): AggregateFunction = {
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

  /**
   * Builds the merge (Final) version of the original result expressions so that each aggregate
   * function reads its partial's buffer column instead of the raw source columns.
   *
   * @param aggExprs the partial-mode aggregate expressions that were pushed down (their buffer
   *                 columns are referenced by resultId)
   * @param resultExprs the original (Complete) result expressions to rewrite
   * @return the rewritten result expressions whose AggregateExpression nodes read the partial
   *         buffers. The outer alias/resultId of each expression is preserved so the output schema
   *         above is unchanged.
   */
  def mergeResultExpressions(
      aggExprs: Seq[AggregateExpression],
      resultExprs: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression])
      : Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression] = {
    val bufferByResultId = aggExprs.map(ae => ae.resultId -> ae).toMap
    resultExprs.map { expr =>
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
      }.asInstanceOf[org.apache.spark.sql.catalyst.expressions.NamedExpression]
    }
  }

  /** Whether all grouping expressions are plain attributes (safe to rebind on a merge). */
  def allPlainAttributeGrouping(agg: Aggregate): Boolean =
    agg.groupingExpressions.nonEmpty &&
      agg.groupingExpressions.forall(_.isInstanceOf[Attribute])
}
