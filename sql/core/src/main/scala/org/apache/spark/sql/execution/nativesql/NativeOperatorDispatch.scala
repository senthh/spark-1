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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

/** Backend tag only. Velox is the intended library later; this layer does not link it. */
sealed trait NativeBackend

object NativeBackend {
  case object VeloxHash extends NativeBackend
  case object ClickHouseSort extends NativeBackend
  case object HybridScan extends NativeBackend
  case object WscgFallback extends NativeBackend
  case object LocalNative extends NativeBackend
}

/**
 * Hybrid operator dispatch over a logical subtree. Does not replace Catalyst.
 */
object NativeOperatorDispatch {

  def decide(plan: LogicalPlan): NativeBackend = {
    if (WscgFallbackGate.shouldFallback(plan)) {
      NativeBackend.WscgFallback
    } else if (firstCore(plan).isInstanceOf[Sort]) {
      NativeBackend.ClickHouseSort
    } else if (isHybridScan(plan)) {
      NativeBackend.HybridScan
    } else if (plan.exists(n => n.isInstanceOf[Aggregate] || n.isInstanceOf[Join])) {
      NativeBackend.VeloxHash
    } else {
      NativeBackend.LocalNative
    }
  }

  /**
   * Hybrid scan rule: enable page-index pruning when the estimated skip ratio
   * is at least `spark.sql.nativesql.scan.pageIndexMinSkipRatio`.
   */
  def pageIndexEnabled(skipRatio: Double): Boolean = {
    skipRatio >= SQLConf.get.nativeSqlPageIndexMinSkipRatio
  }

  @tailrec
  private def firstCore(plan: LogicalPlan): LogicalPlan = plan match {
    case Project(_, child) => firstCore(child)
    case Filter(_, child) => firstCore(child)
    case other => other
  }

  /** File relation leaf plus Filter/Project only (no Sort / Aggregate / Join). */
  private def isHybridScan(plan: LogicalPlan): Boolean = {
    val hasFile = plan.exists(_.isInstanceOf[LogicalRelation])
    val scanFilterProjectOnly = !plan.exists {
      case _: LogicalRelation | _: Filter | _: Project => false
      case _ => true
    }
    hasFile && scanFilterProjectOnly
  }
}
