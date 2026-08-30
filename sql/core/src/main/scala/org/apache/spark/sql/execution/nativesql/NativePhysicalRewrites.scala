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

package org.apache.spark.sql.execution.nativesql

import java.time.LocalDate

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DateType, IntegerType}

/**
 * Safe, local, idempotent logical rewrites applied before native compile.
 * Never changes semantics when unsure. Does not throw.
 */
object NativePhysicalRewrites {

  def rewrite(plan: LogicalPlan): LogicalPlan = {
    try {
      val step1 = plan.transformUp(convertOuterJoinToInner)
      val step2 = step1.transformUp(mergeFilterIntoJoin)
      val step3 = step2.transformUp(dropUnusedProject)
      step3.transformUp(datePreimageYearEq)
    } catch {
      case NonFatal(_) => plan
    }
  }

  /**
   * Filter above an outer join whose predicate rejects null-extended rows
   * (IsNotNull on the nullable side, or comparisons that fail on null) becomes Inner.
   */
  private val convertOuterJoinToInner: PartialFunction[LogicalPlan, LogicalPlan] = {
    case f @ Filter(cond, j @ Join(_, _, LeftOuter | RightOuter | FullOuter, _, _)) =>
      tryConvertOuter(f, cond, j)
  }

  /**
   * Filter(EqualTo(leftAttr, rightAttr)) above Cross/Inner join is folded into the
   * join condition. Cross with an equi-predicate is rewritten as Inner.
   */
  private val mergeFilterIntoJoin: PartialFunction[LogicalPlan, LogicalPlan] = {
    case f @ Filter(cond, j @ Join(left, right, Inner | Cross, existing, _)) =>
      tryMergeEqFilter(f, cond, j, left, right, existing)
  }

  /**
   * Drop unused Project outputs that sit immediately under Project/Aggregate.
   * Conservative: leave the plan unchanged when references are unclear.
   */
  private val dropUnusedProject: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p @ Project(parentList, child: Project) =>
      pruneInnerProject(p, parentList, child, (kept: Seq[NamedExpression]) =>
        p.copy(child = child.copy(projectList = kept)))
    case a @ Aggregate(grouping, agg, child: Project, _) =>
      pruneInnerProject(a, grouping ++ agg, child, (kept: Seq[NamedExpression]) =>
        a.copy(child = child.copy(projectList = kept)))
  }

  /**
   * Filter(EqualTo(Year(dateCol), lit)) -> range on the raw DateType column.
   */
  private val datePreimageYearEq: PartialFunction[LogicalPlan, LogicalPlan] = {
    case f @ Filter(cond, child) =>
      tryDatePreimage(f, cond, child)
  }

  private def tryConvertOuter(orig: Filter, cond: Expression, join: Join): LogicalPlan = {
    try {
      val leftRejects = rejectsNullExtended(cond, join.left.outputSet)
      val rightRejects = rejectsNullExtended(cond, join.right.outputSet)
      val newType = join.joinType match {
        case LeftOuter if rightRejects => Inner
        case RightOuter if leftRejects => Inner
        case FullOuter if leftRejects && rightRejects => Inner
        case other => other
      }
      if (newType == join.joinType) orig else Filter(cond, join.copy(joinType = newType))
    } catch {
      case NonFatal(_) => orig
    }
  }

  private def tryMergeEqFilter(
      orig: Filter,
      cond: Expression,
      join: Join,
      left: LogicalPlan,
      right: LogicalPlan,
      existing: Option[Expression]): LogicalPlan = {
    try {
      val parts = splitAnd(cond)
      val (eqs, rest) = parts.partition(isSidesEquality(_, left, right))
      if (eqs.isEmpty) {
        orig
      } else {
        val merged = (existing.toSeq ++ eqs).reduce(And)
        val newJoin = join.copy(joinType = Inner, condition = Some(merged))
        if (rest.isEmpty) newJoin else Filter(rest.reduce(And), newJoin)
      }
    } catch {
      case NonFatal(_) => orig
    }
  }

  private def pruneInnerProject(
      orig: LogicalPlan,
      parentExprs: Iterable[Expression],
      inner: Project,
      rebuild: Seq[NamedExpression] => LogicalPlan): LogicalPlan = {
    try {
      if (parentExprs.exists(!_.resolved) || inner.projectList.exists(!_.resolved)) {
        return orig
      }
      val needed = AttributeSet(parentExprs.flatMap(_.references))
      val kept = inner.projectList.filter { ne =>
        !ne.deterministic || needed.contains(ne.toAttribute)
      }
      if (kept.isEmpty || kept.size == inner.projectList.size) orig else rebuild(kept)
    } catch {
      case NonFatal(_) => orig
    }
  }

  private def tryDatePreimage(orig: Filter, cond: Expression, child: LogicalPlan): LogicalPlan = {
    try {
      val parts = splitAnd(cond)
      var changed = false
      val rewritten = parts.flatMap { p =>
        yearEqToRange(p) match {
          case Some(range) =>
            changed = true
            splitAnd(range)
          case None => Seq(p)
        }
      }
      if (!changed) orig else Filter(rewritten.reduce(And), child)
    } catch {
      case NonFatal(_) => orig
    }
  }

  private def yearEqToRange(e: Expression): Option[Expression] = e match {
    case EqualTo(y: Year, lit: Literal) => yearRange(y, lit)
    case EqualTo(lit: Literal, y: Year) => yearRange(y, lit)
    case _ => None
  }

  private def yearRange(yearExpr: Year, lit: Literal): Option[Expression] = {
    if (yearExpr.child.dataType != DateType || lit.dataType != IntegerType) {
      return None
    }
    val year = lit.value match {
      case i: java.lang.Integer => i.intValue()
      case i: Int => i
      case _ => return None
    }
    try {
      val start = LocalDate.of(year, 1, 1).toEpochDay.toInt
      val end = LocalDate.of(year + 1, 1, 1).toEpochDay.toInt
      Some(And(
        GreaterThanOrEqual(yearExpr.child, Literal(start, DateType)),
        LessThan(yearExpr.child, Literal(end, DateType))))
    } catch {
      case NonFatal(_) => None
    }
  }

  /** True if `expr` rejects rows that are null-extended on `nullableSide`. */
  private def rejectsNullExtended(expr: Expression, nullableSide: AttributeSet): Boolean = {
    splitAnd(expr).exists {
      case IsNotNull(a: Attribute) => nullableSide.contains(a)
      case Not(IsNull(a: Attribute)) => nullableSide.contains(a)
      case c: BinaryComparison =>
        c.references.nonEmpty && c.references.subsetOf(nullableSide)
      case _ => false
    }
  }

  private def isSidesEquality(e: Expression, left: LogicalPlan, right: LogicalPlan): Boolean = {
    e match {
      case EqualTo(l: Attribute, r: Attribute) =>
        (left.outputSet.contains(l) && right.outputSet.contains(r)) ||
          (left.outputSet.contains(r) && right.outputSet.contains(l))
      case _ => false
    }
  }

  private def splitAnd(e: Expression): Seq[Expression] = e match {
    case And(l, r) => splitAnd(l) ++ splitAnd(r)
    case other => Seq(other)
  }
}
