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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, ExpressionSet, IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, TreePattern}

/**
 * A logical operator that represents a partial (pre-) aggregation.
 *
 * A partial aggregation collapses rows by grouping on `groupingExpressions` and computing the
 * partial results of the given (commutative and associative) aggregate functions. It is an
 * *optional* operator: it does not change the final result of a query, because a real (Final)
 * aggregate above it is always responsible for producing the fully aggregated value. Dropping
 * any PartialAggregate from a plan never affects correctness; keeping one collapses rows cheaply
 * (one hash-map build) before costly operators such as exchanges, joins, or unions.
 *
 * This is the logical counterpart of the physical (pre-shuffle) partial aggregate that Spark's
 * physical planner already inserts for every group-by's own input (see
 * `org.apache.spark.sql.execution.aggregate.AggUtils.planAggregateWithoutDistinct`). The purpose
 * of exposing it as a first-class *logical* operator is to let the Catalyst optimizer place it
 * mid-tree (e.g. below joins and unions), a location the fixed physical partial/final split cannot
 * express. It is lowered back to the existing physical partial `HashAggregateExec` at planning
 * time, so native (Gluten/Velox) execution of partial aggregation is reused unchanged.
 *
 * All `aggregateExpressions` carried by this operator are in `Partial` mode. For correctness it
 * must only aggregate (commutative, associative) functions whose partial results can be merged by
 * a matching Final aggregate.
 */
case class PartialAggregate(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    // The partial aggregate emits the grouping keys followed by the immutable aggregation-buffer
    // attributes of its (Partial-mode) aggregate functions. These stable attribute references are
    // tied to the function objects, so the Final merge above - which redirects the equivalent
    // Complete-mode functions to read these exact buffer columns - binds by construction. This is
    // the same buffer-contract the physical pre-shuffle partial aggregate produces.
    val bufferAttributes = aggregateExpressions.collect {
      case ae: AggregateExpression => ae.aggregateFunction.aggBufferAttributes
    }.flatten
    groupingAttributes ++ bufferAttributes
  }

  override lazy val validConstraints: ExpressionSet = {
    // Grouping keys are deduplicated. Grouping columns are non-null after grouping, mirroring
    // Aggregate's constraint derivation.
    val groupingSet = groupingExpressions.toSet
    ExpressionSet(groupingSet.map(e => IsNotNull(e)).toSeq)
  }

  override lazy val maxRows: Option[Long] = {
    if (groupingExpressions.isEmpty) {
      Some(1L)
    } else {
      child.maxRows
    }
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(AGGREGATE)

  override lazy val references: AttributeSet = {
    AttributeSet(groupingExpressions ++ aggregateExpressions.flatMap(_.references))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): PartialAggregate =
    copy(child = newChild)

  /** Whether this optimizer-visible partial aggregate can be safely removed (always true). */
  def isOptional: Boolean = true
}
