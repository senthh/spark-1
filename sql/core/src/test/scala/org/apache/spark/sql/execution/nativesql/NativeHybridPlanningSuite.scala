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
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * In-memory planning tests for the hybrid native rewrite / dispatch / WSCG gate.
 * Does not load the JNI native library.
 */
class NativeHybridPlanningSuite extends SharedSparkSession {

  private def rel(name: String): LocalRelation =
    LocalRelation(AttributeReference(name, IntegerType)())

  private def innerJoin(left: LogicalPlan, right: LogicalPlan): Join =
    Join(left, right, Inner, None, JoinHint.NONE)

  private def threeJoins: Join = {
    innerJoin(innerJoin(innerJoin(rel("a"), rel("b")), rel("c")), rel("d"))
  }

  test("3 nested inner joins shouldFallback at default depth 3") {
    assert(WscgFallbackGate.consecutiveJoinDepth(threeJoins) === 3)
    assert(WscgFallbackGate.shouldFallback(threeJoins))
  }

  test("1 join shouldFallback is false") {
    val one = innerJoin(rel("a"), rel("b"))
    assert(WscgFallbackGate.consecutiveJoinDepth(one) === 1)
    assert(!WscgFallbackGate.shouldFallback(one))
  }

  test("Filter IsNotNull on right of LeftOuter becomes Inner") {
    val left = LocalRelation(
      AttributeReference("lk", IntegerType)(),
      AttributeReference("lv", IntegerType)())
    val right = LocalRelation(
      AttributeReference("rk", IntegerType)(),
      AttributeReference("rv", IntegerType)())
    val join = Join(
      left,
      right,
      LeftOuter,
      Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    val plan = Filter(IsNotNull(right.output(1)), join)
    val rewritten = NativePhysicalRewrites.rewrite(plan)
    val joins = rewritten.collect { case j: Join => j }
    assert(joins.size === 1)
    assert(joins.head.joinType === Inner)
  }

  test("Filter equality over Cross join is merged into join condition") {
    val left = rel("lk")
    val right = rel("rk")
    val join = Join(left, right, Cross, None, JoinHint.NONE)
    val plan = Filter(EqualTo(left.output.head, right.output.head), join)
    val rewritten = NativePhysicalRewrites.rewrite(plan)
    val j = rewritten.collectFirst { case x: Join => x }.get
    assert(j.condition.isDefined)
    assert(j.condition.get.exists(_.isInstanceOf[EqualTo]))
  }

  test("Sort root dispatches ClickHouseSort") {
    val scan = rel("id")
    val sort = Sort(Seq(SortOrder(scan.output.head, Ascending)), global = true, scan)
    assert(NativeOperatorDispatch.decide(sort) === NativeBackend.ClickHouseSort)
  }

  test("Aggregate dispatches VeloxHash") {
    val scan = rel("id")
    val agg = Aggregate(Seq(scan.output.head), Seq(scan.output.head), scan)
    assert(NativeOperatorDispatch.decide(agg) === NativeBackend.VeloxHash)
  }

  test("3 joins dispatch WscgFallback") {
    assert(NativeOperatorDispatch.decide(threeJoins) === NativeBackend.WscgFallback)
  }

  test("pageIndexEnabled uses min skip ratio") {
    assert(!NativeOperatorDispatch.pageIndexEnabled(0.2))
    assert(NativeOperatorDispatch.pageIndexEnabled(0.8))
  }
}
