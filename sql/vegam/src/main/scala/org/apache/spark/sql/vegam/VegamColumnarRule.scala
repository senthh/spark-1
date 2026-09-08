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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vegam.exec.VegamBackend
import org.apache.spark.sql.vegam.plan.NativeStageCutter
import org.apache.spark.sql.vegam.plan.NativeStageCutter.{CutOk, CutSkip}

/**
 * Rewrites a fully-supported stage to [[NativeStageExec]]. If any node in the
 * stage cannot lower, the original Spark plan is kept.
 */
class VegamColumnarRule extends ColumnarRule with Logging {

  override def preColumnarTransitions: Rule[SparkPlan] = {
    new Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = {
        val conf = SQLConf.get
        if (!VegamConf.enabled(conf)) {
          return plan
        }
        val requested = VegamConf.backend(conf)
        if (VegamBackend.resolve(requested).isEmpty && requested != "auto") {
          skip("backend", requested)
          return plan
        }
        plan.transformDown {
          case node if node.isInstanceOf[HashAggregateExec] || node.isInstanceOf[WindowExec] =>
            NativeStageCutter.cut(node) match {
              case CutOk(native) =>
                VegamBackend.pick(requested, native) match {
                  case Some(name) =>
                    NativeStageExec(native, node.output, name)
                  case None =>
                    skip("backend-unsupported", native.getClass.getSimpleName)
                    node
                }
              case CutSkip(why, detail) =>
                skip(why, detail)
                node
            }
        }
      }
    }
  }

  private def skip(why: String, detail: String): Unit = {
    val msg = s"vegam: skip $why $detail"
    logInfo(msg)
    System.err.println(msg)
  }
}
