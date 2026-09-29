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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.{Join, PartialAggregate}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Validates the partial (pre-)aggregate push-down below a [[Join]].
 *
 * Correctness is the top concern: a partial aggregate is optional and must never change the
 * result. Every query runs with the optimization on and off and the results are compared. On top
 * of that, plan-shape assertions confirm a `PartialAggregate` is pushed directly under the join
 * (as the join's left child), and negative cases confirm the rule declines unsafe shapes (outer
 * joins, cross-input aggregate arguments, left-referencing non-equi conditions).
 *
 * Tables are scan-backed (parquet) so the rule - which deliberately skips in-memory
 * LocalRelation/Range arms by design - actually fires.
 */
class PushPartialAggregateThroughJoinSuite
    extends QueryTest
    with SharedSparkSession {
  import testImplicits._

  private def withOptimization[A](enabled: Boolean)(f: => A): A =
    if (enabled) {
      // Force the partial to fire (threshold 1.0 bypasses the cost gate) so plan-shape assertions
      // and the correctness run both exercise an actually-pushed-down plan. The strict-on-unknown
      // cost gate is verified separately (see the default-threshold negative test).
      withSQLConf(
        SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_THRESHOLD.key -> "1.0")(f)
    } else {
      withSQLConf(SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED.key -> "false")(f)
    }

  /** Runs `query` with the optimization on and off and asserts the results agree. */
  private def assertCorrectness(query: String, name: String): Unit = {
    val expected = withOptimization(enabled = false) { sql(query).collect() }
    val actual = withOptimization(enabled = true) { sql(query).collect() }
    val expectedClean = expected.map(_.toSeq.map(normalizeDouble))
    val actualClean = actual.map(_.toSeq.map(normalizeDouble))
    assert(actualClean.sortBy(_.mkString(",")) ===
      expectedClean.sortBy(_.mkString(",")), s"results differ for $name")
  }

  private def normalizeDouble(v: Any): Any = v match {
    case d: Double => BigDecimal(d).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
    case a: Array[_] => a.map(normalizeDouble)
    case other => other
  }

  private def scanDir(): java.io.File = Utils.createTempDir(namePrefix = "pa_join")

  private def withScan(viewName: String, dir: java.io.File, df: DataFrame)(f: => Unit): Unit = {
    val path = new java.io.File(dir, viewName).getAbsolutePath
    df.write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(viewName)
    f
  }

  /** True if some Join in the plan has a PartialAggregate as its left child. */
  private def hasPartialUnderJoin(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean =
    plan.exists {
      case j: Join => j.left.exists(_.isInstanceOf[PartialAggregate])
      case _ => false
    }

  /** True if some Join in the plan has a PartialAggregate under either input. */
  private def hasPartialOnEitherSide(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean =
    plan.exists {
      case j: Join =>
        j.left.exists(_.isInstanceOf[PartialAggregate]) ||
          j.right.exists(_.isInstanceOf[PartialAggregate])
      case _ => false
    }

  test("one-sided star shape: SUM over inner join grouped by fact column, partial pushed, results equal") {
    val dir = scanDir()
    try {
      // fact: (fid, dkey, amount) - multiple facts per dim; dim: (dkey, dimname). Grouping is on the
      // fact (left) side, so the whole group-by lives on one input - the one-sided case we support.
      val fact = Seq((1, 10, 5), (2, 10, 7), (3, 11, 3), (4, 12, 9), (5, 12, 1)).toDF("fid", "dkey", "amt")
      val dim = Seq((10, "a"), (11, "b"), (12, "c")).toDF("dkey", "dname")
      withScan("fact", dir, fact) { withScan("dim", dir, dim) {
        val q = """SELECT f.dkey, SUM(f.amt) AS s
                  |FROM fact f JOIN dim d ON f.dkey = d.dkey
                  |GROUP BY f.dkey ORDER BY f.dkey""".stripMargin
        assertCorrectness(q, "one-sided star sum join")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(hasPartialUnderJoin(optimized), s"expected PartialAggregate under join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("multiplicity preserved: one-to-many dim side (join key not in grouping), results equal") {
    val dir = scanDir()
    try {
      // fact rows fan out because dim has duplicate keys - the partial must NOT collapse across keys.
      val fact = Seq((1, 20, 5), (2, 20, 7), (3, 20, 3)).toDF("fid", "dkey", "amt")
      val dim = Seq((20, "x"), (20, "y"), (20, "z")).toDF("dkey", "dname")
      withScan("fact2", dir, fact) { withScan("dim2", dir, dim) {
        val q = """SELECT f.fid, SUM(f.amt) AS s, COUNT(*) AS c
                  |FROM fact2 f JOIN dim2 d ON f.dkey = d.dkey
                  |GROUP BY f.fid ORDER BY f.fid""".stripMargin
        // Each (dkey=20) fact row is multiplied by the 3 matching dim rows: per fid SUM(amt)*3,
        // COUNT=3 - only correct if the partial keeps the dkey (join key) in its grouping.
        assertCorrectness(q, "multiplicity sum count")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(hasPartialUnderJoin(optimized), s"expected PartialAggregate under join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("group spans multiple join keys + dim multiplication - still correct") {
    val dir = scanDir()
    try {
      // One fact group (g=u) spans two join keys (20,30); partial must keep both keys separately,
      // and dim has duplicate keys so each (g,k) fan-out is also multiplied.
      val fact = Seq((1, 20, 1, "u"), (2, 30, 2, "u"), (3, 20, 3, "u"), (4, 30, 4, "u")).toDF("fid", "dkey", "amt", "g")
      val dim = Seq((20, "x"), (20, "y"), (30, "z")).toDF("dkey", "dname")
      withScan("fact3", dir, fact) { withScan("dim3", dir, dim) {
        val q = """SELECT f.g, SUM(f.amt) AS s, COUNT(*) AS c
                  |FROM fact3 f JOIN dim3 d ON f.dkey = d.dkey
                  |GROUP BY f.g ORDER BY f.g""".stripMargin
        assertCorrectness(q, "group spans keys")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(hasPartialUnderJoin(optimized),
          s"expected PartialAggregate under join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("outer join: not pushed (no partial below join) and results unchanged") {
    val dir = scanDir()
    try {
      val fact = Seq((1, 10, 5), (2, 99, 7)).toDF("fid", "dkey", "amt")
      val dim = Seq((10, "a")).toDF("dkey", "dname")
      withScan("fact4", dir, fact) { withScan("dim4", dir, dim) {
        val q = """SELECT dname, SUM(f.amt) AS s
                  |FROM fact4 f LEFT JOIN dim4 d ON f.dkey = d.dkey
                  |GROUP BY dname ORDER BY dname""".stripMargin
        assertCorrectness(q, "left outer join")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(!hasPartialUnderJoin(optimized),
          s"expected NO PartialAggregate under left-outer join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("aggregate argument from both sides: not pushed") {
    val dir = scanDir()
    try {
      val l = Seq((1, 10, 5), (2, 20, 7)).toDF("fid", "dkey", "amt")
      val r = Seq((10, 1), (20, 2)).toDF("dkey", "rw")
      withScan("l5", dir, l) { withScan("r5", dir, r) {
        // SUM(f.amt * r.rw) references BOTH inputs => cannot push a single-side partial.
        val q = """SELECT f.fid, SUM(f.amt * r.rw) AS s
                  |FROM l5 f JOIN r5 r ON f.dkey = r.dkey GROUP BY f.fid""".stripMargin
        assertCorrectness(q, "cross-input agg arg")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(!hasPartialUnderJoin(optimized),
          s"expected NO PartialAggregate under cross-input agg join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("non-equi left-referencing join condition: not pushed") {
    val dir = scanDir()
    try {
      val l = Seq((1, 10, 5), (2, 20, 7)).toDF("fid", "dkey", "amt")
      val r = Seq((15, 100)).toDF("rk", "rcap")
      withScan("l6", dir, l) { withScan("r6", dir, r) {
        // Join on l.amt < r.rcap (non-equi referencing the left amt column, not carried by partial).
        val q = """SELECT f.dkey, COUNT(*) AS c
                  |FROM l6 f JOIN r6 r ON f.amt < r.rcap GROUP BY f.dkey""".stripMargin
        assertCorrectness(q, "non-equi left condition")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(!hasPartialUnderJoin(optimized),
          s"expected NO PartialAggregate under non-equi join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("two-side split: GROUP BY dim column (grouping on right), aggregates on left, both partials fire") {
    val dir = scanDir()
    try {
      val fact = Seq((1, 10, 5), (2, 10, 7), (3, 11, 3), (4, 12, 9), (5, 12, 1)).toDF("fid", "dkey", "amt")
      val dim = Seq((10, "a"), (11, "b"), (12, "c")).toDF("dkey", "dname")
      withScan("f2s", dir, fact) { withScan("d2s", dir, dim) {
        // Grouping key dname is on the RIGHT (dim) input; SUM(amt) on the left (fact). The rule must
        // split: partial keyed by (dkey) on the fact, by (dname, dkey) on the dim, merge by dname.
        val q = """SELECT d.dname, SUM(f.amt) AS s
                  |FROM f2s f JOIN d2s d ON f.dkey = d.dkey
                  |GROUP BY d.dname ORDER BY d.dname""".stripMargin
        assertCorrectness(q, "two-side split star")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(hasPartialOnEitherSide(optimized),
          s"expected a PartialAggregate under the two-side join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("two-side split with grouping strings across both dim keys mapping to same name") {
    val dir = scanDir()
    try {
      // Dim has two dkeys (20,30) sharing the SAME dname "u" - the dim partial must keep both keys
      // separate so the join/merge multiplicity is correct.
      val fact = Seq((1, 20, 1), (2, 30, 2), (3, 20, 3), (4, 30, 4)).toDF("fid", "dkey", "amt")
      val dim = Seq((20, "u"), (30, "u")).toDF("dkey", "dname")
      withScan("f3s", dir, fact) { withScan("d3s", dir, dim) {
        val q = """SELECT d.dname, SUM(f.amt) AS s
                  |FROM f3s f JOIN d3s d ON f.dkey = d.dkey
                  |GROUP BY d.dname ORDER BY d.dname""".stripMargin
        assertCorrectness(q, "two-side split multi-key group")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(hasPartialOnEitherSide(optimized),
          s"expected a PartialAggregate under the two-side join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("two-side split: fact side fan-out (dim duplicate keys) still correct") {
    val dir = scanDir()
    try {
      // Dim has duplicate dkey=20 (x,y,z); each fact row fans out x3. The dim partial keyed by
      // (dname, dkey) still keeps 3 dim rows distinct per dkey, so the join multiplicity holds.
      val fact = Seq((1, 20, 5), (2, 20, 7)).toDF("fid", "dkey", "amt")
      val dim = Seq((20, "a"), (20, "a"), (20, "a")).toDF("dkey", "dname")
      withScan("f4s", dir, fact) { withScan("d4s", dir, dim) {
        val q = """SELECT d.dname, SUM(f.amt) AS s
                  |FROM f4s f JOIN d4s d ON f.dkey = d.dkey
                  |GROUP BY d.dname ORDER BY d.dname""".stripMargin
        // SUM(amt) = (5+7) * 3 = 36; COUNT would be 2*3 = 6. Only 0 errors, rows match, is the contract.
        assertCorrectness(q, "two-side split fan-out")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(hasPartialOnEitherSide(optimized),
          s"expected a PartialAggregate under the two-side join in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("aggregates on BOTH sides: not pushed (would multiply the raw-side measure)") {
    val dir = scanDir()
    try {
      val l = Seq((1, 10, 5), (2, 20, 7)).toDF("fid", "dkey", "amt")
      val r = Seq((10, "a", 100), (20, "b", 200)).toDF("dkey", "dname", "rw")
      withScan("l8", dir, l) { withScan("r8", dir, r) {
        // SUM(f.amt) on the fact AND SUM(r.rw) on the dim: collapsing one side would corrupt the
        // measure on the other (it would be multiplied by the collapse ratio). Must not push.
        val q = """SELECT dname, SUM(f.amt) AS s, SUM(r.rw) AS w
                  |FROM l8 f JOIN r8 r ON f.dkey = r.dkey
                  |GROUP BY dname ORDER BY dname""".stripMargin
        assertCorrectness(q, "both-sides aggregates")
        val optimized = withOptimization(enabled = true) { sql(q).queryExecution.optimizedPlan }
        assert(!hasPartialOnEitherSide(optimized),
          s"expected NO PartialAggregate (both sides aggregate) in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("strict cost gate: no distinctCount stats -> partial NOT fired at default threshold") {
    val dir = scanDir()
    try {
      val l = Seq((1, 10, 5), (2, 10, 7), (3, 11, 3)).toDF("fid", "dkey", "amt")
      val r = Seq((10, "a")).toDF("dkey", "dname")
      withScan("lsg", dir, l) { withScan("rsg", dir, r) {
        val q = """SELECT dname, SUM(f.amt) AS s FROM lsg f JOIN rsg r ON f.dkey = r.dkey
                  |GROUP BY dname""".stripMargin
        // These parquet views have NO column statistics (distinctCount), so the strict gate cannot
        // prove a row collapse and must NOT insert the extra pre-aggregate stage.
        withSQLConf(
          SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_THRESHOLD.key -> "0.5") {
          val optimized = sql(q).queryExecution.optimizedPlan
          assert(!hasPartialOnEitherSide(optimized),
            s"expected NO PartialAggregate without distinctCount stats in:\n$optimized")
        }
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("with optimization disabled, no PartialAggregate is introduced") {
    val dir = scanDir()
    try {
      val l = Seq((1, 10, 5), (2, 20, 7)).toDF("fid", "dkey", "amt")
      val r = Seq((10, "a")).toDF("dkey", "dname")
      withScan("l7", dir, l) { withScan("r7", dir, r) {
        val q = """SELECT dname, SUM(f.amt) AS s FROM l7 f JOIN r7 r ON f.dkey = r.dkey
                  |GROUP BY dname""".stripMargin
        val optimized = withOptimization(enabled = false) { sql(q).queryExecution.optimizedPlan }
        assert(!optimized.exists(_.isInstanceOf[PartialAggregate]),
          s"expected no PartialAggregate when disabled in:\n$optimized")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }
}