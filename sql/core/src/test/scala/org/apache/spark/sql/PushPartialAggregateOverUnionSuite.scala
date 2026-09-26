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

import org.apache.spark.sql.catalyst.plans.logical.PartialAggregate
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Validates the partial (pre-)aggregation push-down below a Union.
 *
 * Correctness is the top concern: a partial aggregate is optional and must never change the
 * result. Every query here is run both with the optimization enabled and disabled and the two
 * result sets are compared for equality (doubles are compared with tolerance). On top of that,
 * plan-shape assertions confirm that a `PartialAggregate` was actually pushed below the union.
 */
class PushPartialAggregateOverUnionSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def withOptimization[A](enabled: Boolean)(f: => A): A =
    withSQLConf(SQLConf.OPTIMIZER_PARTIAL_AGGREGATE_PUSHDOWN_ENABLED.key -> enabled.toString)(f)

  /** Registers `data` as a scan-backed temp view (parquet) so the partial push-down actually fires.
   *  LocalRelation-backed views (toDF) are skipped by the rule by design (ConvertToLocalRelation
   *  would collapse the tiny in-memory data and regenerate the partial's buffer exprIds). */
  private def withScan(viewName: String, dir: java.io.File, df: DataFrame)(f: => Unit): Unit = {
    val path = new java.io.File(dir, viewName).getAbsolutePath
    df.write.mode("overwrite").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(viewName)
    f
  }

  private def scanDir(): java.io.File = Utils.createTempDir(namePrefix = "pa_scan")

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

  /** Asserts the optimized plan contains a `PartialAggregate` pushed below a Union. */
  private def assertPartialBelowUnion(df: DataFrame): Unit = {
    val optimized = withOptimization(enabled = true) { df.queryExecution.optimizedPlan }
    assert(hasPartialUnderUnion(optimized),
      s"expected a PartialAggregate pushed below a Union in:\n$optimized")
  }

  /** Whether some Union in the tree has a PartialAggregate before (below) one of its inputs. */
  private def hasPartialUnderUnion(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean = plan match {
    case u: org.apache.spark.sql.catalyst.plans.logical.Union =>
      // A pushed-down partial is under the union: the union's relationship to its children is
      // such that a PartialAggregate ancestor of a child (below the union) counts as pushed-down.
      u.children.exists(childExistsBelowUnion)
    case other =>
      other.children.exists(hasPartialUnderUnion)
  }

  /** Whether the given plan or its subtree contains a PartialAggregate *below* it. */
  private def childExistsBelowUnion(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Boolean =
    plan.exists(_.isInstanceOf[PartialAggregate])

  test("SUM over UNION ALL - results equal with optimization on/off, partial pushed") {
    val dir = scanDir()
    try {
      val t1 = Seq((1, 5), (1, 7), (2, 1), (3, 0)).toDF("k", "v")
      val t2 = Seq((1, 9), (2, 4), (2, 6), (4, 2)).toDF("k", "v")
      withScan("t1", dir, t1) { withScan("t2", dir, t2) {
        val q = """SELECT k, SUM(v) AS s FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                  |GROUP BY k ORDER BY k""".stripMargin
        assertCorrectness(q, "sum union")
        assertPartialBelowUnion(sql(q))
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("MIN/MAX/COUNT over UNION ALL") {
    val dir = scanDir()
    try {
      val t1 = Seq((1, 5), (1, 7), (2, 1), (3, 0)).toDF("k", "v")
      val t2 = Seq((1, 9), (2, 4), (2, 6), (4, 2)).toDF("k", "v")
      withScan("t1", dir, t1) { withScan("t2", dir, t2) {
        val q = """SELECT k, MIN(v) AS mn, MAX(v) AS mx, COUNT(*) AS c
                  |FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                  |GROUP BY k ORDER BY k""".stripMargin
        assertCorrectness(q, "min max count union")
        assertPartialBelowUnion(sql(q))
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("all-null group stays NULL under partial SUM merge") {
    val dir = scanDir()
    try {
      // t1 has a group k=1 with all-null v; t2 has no row for k=1. Original result must be NULL.
      val t1 = Seq((1, None), (2, Some(3))).toDF("k", "v")
      val t2 = Seq((2, Some(4)), (3, Some(1))).toDF("k", "v")
      withScan("t1", dir, t1) { withScan("t2", dir, t2) {
        val q = """SELECT k, SUM(v) AS s FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                  |GROUP BY k ORDER BY k""".stripMargin
        assertCorrectness(q, "all-null sum union")
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("UNION ALL of overlapping groups with all functions") {
    val dir = scanDir()
    try {
      val t1 = Seq((1, None), (1, Some(5)), (2, Some(3)), (3, None)).toDF("k", "v")
      val t2 = Seq((1, Some(2)), (3, Some(7)), (3, Some(0)), (4, None)).toDF("k", "v")
      withScan("t1", dir, t1) { withScan("t2", dir, t2) {
        val q = """SELECT k, SUM(v) AS s, COUNT(v) AS c, MIN(v) AS mn, MAX(v) AS mx
                  |FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                  |GROUP BY k ORDER BY k""".stripMargin
        assertCorrectness(q, "overlapping groups union")
        assertPartialBelowUnion(sql(q))
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("DECIMAL SUM over UNION ALL - all-empty group stays NULL, partial fired") {
    val dir = scanDir()
    try {
      // t1 has a group with all-null decimal v (must stay NULL, not 0); t2 contributes to it too.
      val t1 = Seq((1, Some(BigDecimal("10.50"))), (2, None), (2, Some(BigDecimal("3.25")))).toDF("k", "v")
      val t2 = Seq((1, Some(BigDecimal("4.00"))), (2, None), (3, Some(BigDecimal("1.10")))).toDF("k", "v")
      withScan("t1", dir, t1) { withScan("t2", dir, t2) {
        val q = """SELECT k, SUM(CAST(v AS DECIMAL(12,2))) AS s
                  |FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                  |GROUP BY k ORDER BY k""".stripMargin
        assertCorrectness(q, "decimal sum union")
        assertPartialBelowUnion(sql(q))
      }}
    } finally { Utils.deleteRecursively(dir) }
  }

  test("q76 shape: stored decimal SUM column + COUNT over 3-way union (DecimalAggregates guard)") {
    val dir = Utils.createTempDir(namePrefix = "pa_q76")
    try {
      def mkd(name: String, v: Seq[(String, String, Int, Int, String, java.math.BigDecimal)]) = {
        v.toDF("channel", "col", "yr", "qoy", "cat", "ext")
          .withColumn("ext2", $"ext".cast("decimal(7,2)")).drop("ext").withColumnRenamed("ext2", "ext")
          .write.mode("overwrite").parquet(new java.io.File(dir, name).getAbsolutePath)
      }
      mkd("s", Seq(
        ("store", "a", 1999, 1, "c1", new java.math.BigDecimal("10.50")),
        ("store", "b", 2000, 2, "c1", new java.math.BigDecimal("1.25"))))
      mkd("w", Seq(("web", "x", 1999, 1, "c1", new java.math.BigDecimal("5.00"))))
      mkd("c", Seq(("catalog", "y", 2000, 2, "c2", new java.math.BigDecimal("9.90"))))
      def read(n: String) = spark.read.parquet(new java.io.File(dir, n).getAbsolutePath)
      read("s").createOrReplaceTempView("s")
      read("w").createOrReplaceTempView("w")
      read("c").createOrReplaceTempView("c")
      val q = """SELECT channel, col, yr, qoy, cat,
                |       COUNT(*) AS sales_cnt, SUM(ext) AS sales_amt
                |FROM (SELECT * FROM s UNION ALL SELECT * FROM w UNION ALL SELECT * FROM c) t
                |GROUP BY channel, col, yr, qoy, cat
                |ORDER BY channel, col, yr, qoy, cat LIMIT 100""".stripMargin
      // Correctness: on/off must agree; and the stored-decimal SUM column must not be clobbered by
      // Spark's DecimalAggregates (which historically rewrote inside the partial and broke it).
      assertCorrectness(q, "q76 shape")
      assertPartialBelowUnion(sql(q))
      // Type of the SUM column is preserved.
      val amtType = withOptimization(enabled = true) { sql(q).schema("sales_amt").dataType.typeName }
      assert(amtType == "decimal(17,2)", s"unexpected sales_amt type: $amtType")
    } finally {
      Utils.deleteRecursively(dir)
    }
  }

  test("with optimization disabled, no PartialAggregate is introduced") {
    withTempView("t1", "t2") {
      Seq((1, 5)).toDF("k", "v").createOrReplaceTempView("t1")
      Seq((1, 9)).toDF("k", "v").createOrReplaceTempView("t2")
      val q = """SELECT k, SUM(v) AS s FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                |GROUP BY k""".stripMargin
      val optimized = withOptimization(enabled = false) { sql(q).queryExecution.optimizedPlan }
      assert(!optimized.exists(_.isInstanceOf[PartialAggregate]), optimized)
    }
  }

  test("physical plan contains a partial HashAggregateExec before the Union") {
    // Use a non-adaptive plan so the Phase-1 (partial) hash aggregates are directly visible.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("t1", "t2") {
        Seq((1, 5), (2, 1)).toDF("k", "v").createOrReplaceTempView("t1")
        Seq((1, 9), (3, 2)).toDF("k", "v").createOrReplaceTempView("t2")
        val q = """SELECT k, SUM(v) AS s FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2)
                  |GROUP BY k""".stripMargin
        val physical = withOptimization(enabled = true) { sql(q).queryExecution.executedPlan }
        import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Partial}
        // The pushed-down partial aggregates execute as Partial-mode hash aggregates directly on
        // each union input (the merge is a separate Partial/Final pair above the union).
        val partialBelowUnion = physical.exists {
          case agg: HashAggregateExec
              if agg.aggregateExpressions.nonEmpty &&
                agg.aggregateExpressions.forall(ae => (ae.mode: AggregateMode) == Partial) => true
          case _ => false
        }
        assert(partialBelowUnion, s"expected a partial hash aggregate in:\n$physical")
      }
    }
  }
}
