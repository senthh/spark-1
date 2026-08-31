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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * Optimizer rule: rewrite Q9-style CASE of uncorrelated scalar COUNT/AVG
 * buckets into one segmented Aggregate before MergeSubplans / AQE.
 */
object RewriteScalarBucketAggs extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.nativeSqlEnabled) {
      plan
    } else {
      try {
        NativeSqlPlan.rewriteScalarBuckets(plan)
      } catch {
        case _: Throwable => plan
      }
    }
  }
}

/**
 * Planning rewrite: Union(Join(a, dim), Join(b, dim)) -> Join(Union(a, b), dim)
 * so Q5-style sales/returns pairs share one date_dim probe.
 */
object LiftCommonScans extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.nativeSqlEnabled) {
      plan
    } else {
      plan.transformUp {
        case u: Union if u.children.size >= 2 =>
          liftUnion(u).getOrElse(u)
      }
    }
  }

  private def liftUnion(u: Union): Option[LogicalPlan] = {
    val peeled = u.children.map(peelJoin)
    if (peeled.exists(_.isEmpty)) {
      None
    } else {
      val joins = peeled.map(_.get)
      val dimKey = scanKey(joins.head.dim)
      val sameDim = dimKey.isDefined && joins.forall(j =>
        scanKey(j.dim) == dimKey &&
          (j.joinType == Inner || j.joinType == LeftOuter) &&
          j.cond.isDefined)
      if (!sameDim) {
        None
      } else {
        val facts = joins.map(_.fact)
        if (facts.exists(_.output.size != facts.head.output.size)) {
          None
        } else {
          val unionFacts = Union(facts)
          val dim = joins.head.dim
          val leftMap = AttributeMap(joins.head.fact.output.zip(unionFacts.output))
          val newCond = joins.head.cond.map(_.transform {
            case a: Attribute if leftMap.contains(a) => leftMap(a)
          })
          val joined = Join(unionFacts, dim, joins.head.joinType, newCond, joins.head.hint)
          Some(reproject(joins, joined))
        }
      }
    }
  }

  private case class PeeledJoin(
      fact: LogicalPlan,
      dim: LogicalPlan,
      joinType: org.apache.spark.sql.catalyst.plans.JoinType,
      cond: Option[Expression],
      hint: JoinHint,
      project: Option[Seq[NamedExpression]],
      filter: Option[Expression])

  private def peelJoin(p: LogicalPlan): Option[PeeledJoin] = {
    def fromJoin(j: Join, proj: Option[Seq[NamedExpression]],
        filt: Option[Expression]): Option[PeeledJoin] = {
      if (j.condition.isEmpty) {
        None
      } else if (isFileScan(j.right) && !isFileScan(j.left)) {
        Some(PeeledJoin(j.left, j.right, j.joinType, j.condition, j.hint, proj, filt))
      } else if (isFileScan(j.left) && !isFileScan(j.right)) {
        Some(PeeledJoin(j.right, j.left, j.joinType, j.condition, j.hint, proj, filt))
      } else if (isFileScan(j.right)) {
        Some(PeeledJoin(j.left, j.right, j.joinType, j.condition, j.hint, proj, filt))
      } else {
        None
      }
    }
    p match {
      case Project(plist, j: Join) => fromJoin(j, Some(plist), None)
      case Filter(f, Project(plist, j: Join)) => fromJoin(j, Some(plist), Some(f))
      case Filter(f, j: Join) => fromJoin(j, None, Some(f))
      case j: Join => fromJoin(j, None, None)
      case SubqueryAlias(_, c) => peelJoin(c)
      case _ => None
    }
  }

  private def reproject(joins: Seq[PeeledJoin], joined: LogicalPlan): LogicalPlan = {
    val head = joins.head
    val withFilt = head.filter.map(f => Filter(f, joined)).getOrElse(joined)
    head.project match {
      case Some(plist) =>
        val map = AttributeMap((head.fact.output ++ head.dim.output).zip(joined.output))
        val newList = plist.map {
          case a: Attribute if map.contains(a) => map(a)
          case Alias(c, n) => Alias(c.transform {
            case a: Attribute if map.contains(a) => map(a)
          }, n)()
          case other => other
        }
        Project(newList, withFilt)
      case None =>
        withFilt
    }
  }

  private def isFileScan(p: LogicalPlan): Boolean = unwrap(p) match {
    case LogicalRelation(_: HadoopFsRelation, _, _, _, _) => true
    case _ => false
  }

  @scala.annotation.tailrec
  private def unwrap(p: LogicalPlan): LogicalPlan = p match {
    case SubqueryAlias(_, c) => unwrap(c)
    case other => other
  }

  private def scanKey(p: LogicalPlan): Option[String] = unwrap(p) match {
    case LogicalRelation(fs: HadoopFsRelation, _, _, _, _) =>
      Some(fs.location.rootPaths.map(_.toString).sorted.mkString("|"))
    case _ => None
  }
}
