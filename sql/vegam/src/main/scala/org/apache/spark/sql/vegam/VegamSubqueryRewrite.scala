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

package org.apache.spark.sql.vegam

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Phase 5: turn leftover Exists / IN-subquery filters into semi or anti
 * joins so the cutter can fuse them as JOIN_SEMI / JOIN_ANTI.
 */
object VegamSubqueryRewrite extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VegamConf.enabled(plan.conf)) {
      plan
    } else {
      plan.transformUp {
        case f @ Filter(cond, child) =>
          rewriteFilter(f, cond, child)
        case other => other
      }
    }
  }

  private def rewriteFilter(orig: Filter, cond: Expression, child: LogicalPlan): LogicalPlan = {
    val parts = splitAnd(cond)
    var cur = child
    val kept = parts.flatMap {
      case InSubquery(values, query) if values.nonEmpty && query.plan.output.nonEmpty =>
        cur = semi(cur, values, query.plan, LeftSemi)
        None
      case Not(InSubquery(values, query)) if values.nonEmpty && query.plan.output.nonEmpty =>
        cur = semi(cur, values, query.plan, LeftAnti)
        None
      case e: Exists =>
        existsKeys(e.plan, cur) match {
          case Some((leftKeys, rightKeys, sub)) =>
            cur = Join(cur, sub, LeftSemi, Some(eqs(leftKeys, rightKeys)), JoinHint.NONE)
            None
          case None => Some(e)
        }
      case Not(e: Exists) =>
        existsKeys(e.plan, cur) match {
          case Some((leftKeys, rightKeys, sub)) =>
            cur = Join(cur, sub, LeftAnti, Some(eqs(leftKeys, rightKeys)), JoinHint.NONE)
            None
          case None => Some(Not(e))
        }
      case other => Some(other)
    }
    if (cur eq child) {
      orig
    } else if (kept.isEmpty) {
      cur
    } else {
      Filter(kept.reduce(And), cur)
    }
  }

  private def semi(
      left: LogicalPlan,
      values: Seq[Expression],
      right: LogicalPlan,
      jt: org.apache.spark.sql.catalyst.plans.JoinType): LogicalPlan = {
    val keys = values.zip(right.output).map { case (l, r) => EqualTo(l, r) }
    Join(left, right, jt, Some(keys.reduce(And)), JoinHint.NONE)
  }

  private def eqs(left: Seq[Expression], right: Seq[Expression]): Expression = {
    left.zip(right).map { case (l, r) => EqualTo(l, r) }.reduce(And)
  }

  private def existsKeys(
      sub: LogicalPlan,
      outer: LogicalPlan): Option[(Seq[Expression], Seq[Expression], LogicalPlan)] = {
    val stripped = sub match {
      case Project(_, c) => c
      case SubqueryAlias(_, c) => c
      case other => other
    }
    stripped match {
      case Filter(cond, child) =>
        val eqs = splitAnd(cond).collect {
          case EqualTo(l, r) if outer.output.exists(_.semanticEquals(l)) => (l, r)
          case EqualTo(l, r) if outer.output.exists(_.semanticEquals(r)) => (r, l)
        }
        if (eqs.isEmpty) {
          None
        } else {
          Some((eqs.map(_._1), eqs.map(_._2), child))
        }
      case _ => None
    }
  }

  private def splitAnd(e: Expression): Seq[Expression] = e match {
    case And(l, r) => splitAnd(l) ++ splitAnd(r)
    case other => Seq(other)
  }
}
