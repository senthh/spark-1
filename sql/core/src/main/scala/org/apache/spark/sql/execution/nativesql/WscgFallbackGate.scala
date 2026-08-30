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

import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Q72/Q95 gate: vector engines lose to WholeStageCodegen on long join pipelines.
 * Counts consecutive [[Join]] nodes on the longest path of a [[LogicalPlan]].
 */
object WscgFallbackGate {

  /**
   * Longest run of consecutive Join nodes. A non-Join operator breaks the chain
   * and the search continues in that subtree.
   */
  def consecutiveJoinDepth(plan: LogicalPlan): Int = plan match {
    case j: Join =>
      1 + math.max(consecutiveJoinDepth(j.left), consecutiveJoinDepth(j.right))
    case other =>
      other.children.map(consecutiveJoinDepth).foldLeft(0)(math.max)
  }

  /** True when consecutive join depth is at least `spark.sql.nativesql.wscgJoinDepth`. */
  def shouldFallback(plan: LogicalPlan): Boolean = {
    consecutiveJoinDepth(plan) >= SQLConf.get.nativeSqlWscgJoinDepth
  }
}
