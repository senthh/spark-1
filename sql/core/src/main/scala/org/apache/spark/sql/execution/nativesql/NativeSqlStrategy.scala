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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, IntegerLiteral, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.{
  FileSourceScanExec, GlobalLimitExec, LocalLimitExec, ProjectExec, SortExec, SparkPlan,
  SparkStrategy, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.datasources.{FileSourceStrategy, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Planner hook: rewrite, then offload a supported subtree to [[NativeSqlExec]].
 *
 * Filter-only file plans stay on Spark so FileSourceStrategy keeps Parquet
 * dataFilters. Join / hashagg / union trees take every file leaf as a child;
 * the largest scan is the probe, the rest are broadcast.
 */
object NativeSqlStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!SQLConf.get.nativeSqlEnabled) {
      Nil
    } else if (SQLConf.get.nativeSqlDispatchEnabled &&
        WscgFallbackGate.shouldFallback(plan) &&
        !plan.exists(NativeSqlPlan.isFileScanLeaf)) {
      Nil
    } else {
      val rewritten = NativePhysicalRewrites.rewrite(plan)
      NativeSqlPlan.compileSharedBucketsPlan(rewritten).flatMap { case (compiled, cases) =>
        val planned = planFileBacked(compiled)
        if (planned.nonEmpty) Some(ProjectExec(cases, planned.head)) else None
      } match {
        case Some(p) => p :: Nil
        case None =>
          NativeSqlPlan.compile(rewritten) match {
            case Some(compiled) if compiled.hasFileLeaf =>
              val planned = planFileBacked(compiled)
              if (planned.nonEmpty) planned else planHybridTop(rewritten)
            case Some(compiled) =>
              NativeSqlExec(compiled, Nil) :: Nil
            case None =>
              planHybridTop(rewritten)
          }
      }
    }
  }

  /**
   * Nodes we do not compile to IR (Sort/Limit, two-fact Union, identity Project)
   * stay in Spark. The child is still offloaded when [[NativeSqlPlan.compile]]
   * succeeds. File-backed Sort stays out of C++ so Spark can TakeOrdered.
   */
  private def planHybridTop(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReturnAnswer(child) =>
      planHybridTop(child)
    case Limit(IntegerLiteral(n), Sort(order, true, child, _)) =>
      planNativeChild(child).map(c => takeOrdered(n, order, child.output, c))
    case Limit(IntegerLiteral(n), Project(plist, Sort(order, true, child, _))) =>
      planNativeChild(child).map(c => takeOrdered(n, order, plist, c))
    case GlobalLimit(IntegerLiteral(n), Sort(order, true, child, _)) =>
      planNativeChild(child).map(c => takeOrdered(n, order, child.output, c))
    case GlobalLimit(IntegerLiteral(n), LocalLimit(IntegerLiteral(_), child)) =>
      planNativeChild(child).map(c => GlobalLimitExec(n, LocalLimitExec(n, c)))
    case GlobalLimit(IntegerLiteral(n), child) =>
      planNativeChild(child).map(c => GlobalLimitExec(n, c))
    case LocalLimit(IntegerLiteral(n), child) =>
      planNativeChild(child).map(c => LocalLimitExec(n, c))
    case Sort(order, global, child, _) =>
      planNativeChild(child).map(c => SortExec(order, global, c))
    case Project(plist, child) =>
      NativeSqlPlan.compileSharedBuckets(plist, child) match {
        case Some((compiled, cases)) =>
          val planned = planFileBacked(compiled)
          if (planned.nonEmpty) ProjectExec(cases, planned.head) :: Nil else Nil
        case None if isIdentityProject(plist) =>
          planNativeChild(child).map(n => ProjectExec(plist, n))
        case None =>
          Nil
      }
    case u: Union =>
      val kids = u.children.map(c => apply(c))
      if (kids.nonEmpty && kids.forall(_.nonEmpty)) {
        UnionExec(kids.map(_.head)) :: Nil
      } else {
        Nil
      }
    case _ =>
      Nil
  }

  private def planNativeChild(child: LogicalPlan): Seq[SparkPlan] = {
    NativeSqlPlan.compile(child) match {
      case Some(compiled) if compiled.hasFileLeaf =>
        val planned = planFileBacked(compiled)
        if (planned.nonEmpty) planned else planHybridTop(child)
      case Some(compiled) =>
        NativeSqlExec(compiled, Nil) :: Nil
      case None =>
        planHybridTop(child)
    }
  }

  private def takeOrdered(
      n: Int,
      order: Seq[SortOrder],
      projectList: Seq[NamedExpression],
      child: SparkPlan): SparkPlan = {
    TakeOrderedAndProjectExec(n, order, projectList, child)
  }

  /** Reorder / rename only. Do not eval substr or other ops on hashed strings. */
  private def isIdentityProject(plist: Seq[NamedExpression]): Boolean = {
    plist.forall {
      case _: Attribute => true
      case Alias(_: Attribute, _) => true
      case _ => false
    }
  }

  private def planFileBacked(compiled: NativeSqlPlan.Compiled): Seq[SparkPlan] = {
    val nativeScan = SQLConf.get.nativeSqlScanEnabled
    if (compiled.isPassthroughScan && !nativeScan) {
      Nil
    } else if (!compiled.isHeavy && !nativeScan) {
      Nil
    } else {
      val rels = compiled.fileRels
      val scans = rels.map(r => extractScan(FileSourceStrategy(r)))
      if (scans.exists(_.isEmpty)) {
        Nil
      } else {
        attachBroadcasts(rels, scans.map(_.get)) match {
          case Some(kids) => NativeSqlExec(compiled, kids) :: Nil
          case None => Nil
        }
      }
    }
  }

  /**
   * Largest leaf is the probe (fact). Every other leaf must be broadcastable
   * (size cap or few files). Two large facts (Q5 sales+returns) stay as
   * separate NativeSqlExecs until empty-sibling union is proven on YARN.
   */
  private def attachBroadcasts(
      rels: Seq[LogicalPlan],
      scans: Seq[FileSourceScanExec]): Option[Seq[SparkPlan]] = {
    if (rels.size <= 1) {
      Some(scans)
    } else {
      val cap = math.max(SQLConf.get.autoBroadcastJoinThreshold, 64L * 1024 * 1024)
      val scored = rels.zipWithIndex.map { case (r, i) =>
        (i, r.stats.sizeInBytes.toLong, fileCount(r))
      }
      val probeIdx = scored.maxBy(s => (s._2, s._3))._1
      val tooBig = scored.exists { case (i, bytes, files) =>
        i != probeIdx && bytes > cap && files > 4
      }
      if (tooBig) {
        None
      } else {
        Some(scans.zipWithIndex.map { case (s, i) =>
          if (i == probeIdx) s else BroadcastExchangeExec(IdentityBroadcastMode, s)
        })
      }
    }
  }

  private def fileCount(p: LogicalPlan): Int = {
    p.collectFirst {
      case LogicalRelation(fs: HadoopFsRelation, _, _, _, _) =>
        try {
          fs.location.inputFiles.length
        } catch {
          case _: Throwable => Int.MaxValue
        }
    }.getOrElse(Int.MaxValue)
  }

  private def extractScan(planned: Seq[SparkPlan]): Option[FileSourceScanExec] = {
    planned.headOption.flatMap { root =>
      root.collectFirst { case s: FileSourceScanExec => s }
    }
  }
}
