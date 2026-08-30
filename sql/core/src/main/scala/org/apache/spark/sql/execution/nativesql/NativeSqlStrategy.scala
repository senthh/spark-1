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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.FileSourceStrategy
import org.apache.spark.sql.internal.SQLConf

/**
 * Planner hook: rewrite, then offload a supported subtree to [[NativeSqlExec]].
 * File-backed plans keep [[FileSourceStrategy]] as the scan child and run C++
 * per partition. Identity file scans are not wrapped.
 */
object NativeSqlStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!SQLConf.get.nativeSqlEnabled) {
      Nil
    } else if (SQLConf.get.nativeSqlDispatchEnabled && WscgFallbackGate.shouldFallback(plan)) {
      Nil
    } else {
      val rewritten = NativePhysicalRewrites.rewrite(plan)
      NativeSqlPlan.compile(rewritten) match {
        case Some(compiled) if compiled.hasFileLeaf =>
          planFileBacked(compiled)
        case Some(compiled) =>
          NativeSqlExec(compiled, Nil) :: Nil
        case None =>
          Nil
      }
    }
  }

  private def planFileBacked(compiled: NativeSqlPlan.Compiled): Seq[SparkPlan] = {
    if (compiled.isPassthroughScan) {
      Nil
    } else {
      compiled.fileRels match {
        case Seq(rel) =>
          val scans = FileSourceStrategy(rel)
          if (scans.isEmpty) {
            Nil
          } else {
            NativeSqlExec(compiled, scans) :: Nil
          }
        case _ =>
          Nil
      }
    }
  }
}
