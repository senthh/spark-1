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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, Inner, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
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

  test("INTERSECT compiles to hashsemi") {
    val left = rel("z")
    val right = rel("z")
    val compiled = NativeSqlPlan.compile(Intersect(left, right, isAll = false))
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashsemi"))
  }

  test("multi-column INTERSECT is not compiled") {
    val left = LocalRelation(
      AttributeReference("a", IntegerType)(),
      AttributeReference("b", IntegerType)())
    val right = LocalRelation(
      AttributeReference("a", IntegerType)(),
      AttributeReference("b", IntegerType)())
    assert(NativeSqlPlan.compile(Intersect(left, right, isAll = false)).isEmpty)
  }

  test("Distinct compiles to hashagg") {
    val compiled = NativeSqlPlan.compile(Distinct(rel("id")))
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashagg"))
  }

  test("Exists filter compiles to hashsemi") {
    val left = rel("id")
    val right = rel("id")
    val exists = Exists(
      right,
      joinCond = Seq(EqualTo(left.output.head, right.output.head)))
    val compiled = NativeSqlPlan.compile(Filter(exists, left))
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashsemi"))
  }

  test("OR of Exists compiles to union + hashsemi") {
    val left = rel("id")
    val r1 = rel("id")
    val r2 = rel("id")
    val e1 = Exists(r1, joinCond = Seq(EqualTo(left.output.head, r1.output.head)))
    val e2 = Exists(r2, joinCond = Seq(EqualTo(left.output.head, r2.output.head)))
    val compiled = NativeSqlPlan.compile(Filter(Or(e1, e2), left))
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashsemi"))
    assert(compiled.get.ir.contains("union"))
  }

  test("ExistenceJoin compiles to hashsemi") {
    val left = rel("id")
    val right = rel("id")
    val existsAttr = AttributeReference("exists", org.apache.spark.sql.types.BooleanType)()
    val join = Join(
      left,
      right,
      ExistenceJoin(existsAttr),
      Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    val compiled = NativeSqlPlan.compile(join)
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashsemi"))
  }

  test("LeftSemi compiles to hashsemi") {
    val left = rel("id")
    val right = rel("id")
    val join = Join(
      left,
      right,
      LeftSemi,
      Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)
    val compiled = NativeSqlPlan.compile(join)
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashsemi"))
  }

  test("uncorrelated scalar IN-equality compiles to hashsemi") {
    val left = rel("id")
    val sub = rel("id")
    val pred = EqualTo(left.output.head, ScalarSubquery(sub))
    val compiled = NativeSqlPlan.compile(Filter(pred, left))
    assert(compiled.isDefined)
    assert(compiled.get.ir.contains("hashsemi"))
  }

  test("Q9-style scalar buckets compile to one segagg") {
    val qty = AttributeReference("ss_quantity", IntegerType)()
    val disc = AttributeReference("ss_ext_discount_amt", IntegerType)()
    val prof = AttributeReference("ss_net_profit", IntegerType)()
    val fact = LocalRelation(qty, disc, prof)
    val reasonCol = AttributeReference("r_reason_sk", IntegerType)()
    val reason = Filter(EqualTo(reasonCol, Literal(1)), LocalRelation(reasonCol))
    def bucket(lo: Int, hi: Int, thresh: Long, name: String): NamedExpression = {
      def qtyF = And(GreaterThanOrEqual(qty, Literal(lo)), LessThanOrEqual(qty, Literal(hi)))
      def cnt = ScalarSubquery(Aggregate(Nil, Seq(Alias(Count(Literal(1)), "c")()), Filter(qtyF, fact)))
      def avg(a: Attribute) =
        ScalarSubquery(Aggregate(Nil, Seq(Alias(Average(a), "a")()), Filter(qtyF, fact)))
      Alias(
        CaseWhen(Seq((GreaterThan(cnt, Literal(thresh)), avg(disc))), Some(avg(prof))),
        name)()
    }
    val plist = Seq(bucket(1, 20, 100L, "bucket1"), bucket(21, 40, 200L, "bucket2"))
    val got = NativeSqlPlan.compileSharedBuckets(plist, reason)
    assert(got.isDefined)
    val (compiled, cases) = got.get
    assert(compiled.ir.contains("segagg"))
    assert(compiled.leaves.size === 1)
    assert(cases.size === 2)
    assert(!compiled.ir.contains("scan 2"))
  }

  test("Q9-style buckets rewrite drops scalar subqueries") {
    val qty = AttributeReference("ss_quantity", IntegerType)()
    val disc = AttributeReference("ss_ext_discount_amt", IntegerType)()
    val prof = AttributeReference("ss_net_profit", IntegerType)()
    val fact = LocalRelation(qty, disc, prof)
    val reasonCol = AttributeReference("r_reason_sk", IntegerType)()
    val reason = Filter(EqualTo(reasonCol, Literal(1)), LocalRelation(reasonCol))
    def bucket(lo: Int, hi: Int, thresh: Long, name: String): NamedExpression = {
      def qtyF = And(GreaterThanOrEqual(qty, Literal(lo)), LessThanOrEqual(qty, Literal(hi)))
      def cnt = ScalarSubquery(Aggregate(Nil, Seq(Alias(Count(Literal(1)), "c")()), Filter(qtyF, fact)))
      def avg(a: Attribute) =
        ScalarSubquery(Aggregate(Nil, Seq(Alias(Average(a), "a")()), Filter(qtyF, fact)))
      Alias(
        CaseWhen(Seq((GreaterThan(cnt, Literal(thresh)), avg(disc))), Some(avg(prof))),
        name)()
    }
    val orig = Project(Seq(bucket(1, 20, 100L, "bucket1"), bucket(21, 40, 200L, "bucket2")), reason)
    val rewritten = NativeSqlPlan.rewriteScalarBuckets(orig)
    assert(!rewritten.exists(_.expressions.exists(_.isInstanceOf[ScalarSubquery])))
    assert(rewritten.exists(_.isInstanceOf[Aggregate]))
    val got = NativeSqlPlan.compileRewrittenBuckets(rewritten)
    assert(got.isDefined)
    assert(got.get._1.ir.contains("segagg"))
    assert(got.get._1.ir.contains("(ge c"))
    assert(got.get._2.size === 2)
  }

  test("LiftCommonScans is a no-op without repeated file scans") {
    withSQLConf(SQLConf.NATIVE_SQL_ENABLED.key -> "true") {
      val a = rel("a")
      val b = rel("b")
      val u = Union(Seq(a, b))
      assert(LiftCommonScans(u) === u)
    }
  }
}
