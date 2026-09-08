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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vegam.exec.NativeTask
import org.apache.spark.sql.vegam.plan.{CountStar, HashAgg, NativePlanCodec}

class VegamPathsSuite extends org.apache.spark.SparkFunSuite {
  test("isPosix accepts local and file URIs only") {
    assert(VegamPaths.isPosix("/tmp/store_sales.parquet"))
    assert(VegamPaths.isPosix("file:/tmp/store_sales.parquet"))
    assert(VegamPaths.isPosix("file:///tmp/store_sales.parquet"))
    assert(!VegamPaths.isPosix("hdfs://nn:8020/tmp/x"))
    assert(!VegamPaths.isPosix("s3a://bucket/x"))
    assert(!VegamPaths.isPosix(""))
  }

  test("isSupported accepts Hadoop remote URIs") {
    assert(VegamPaths.isSupported("hdfs://nn:8020/tmp/x"))
    assert(VegamPaths.isSupported("viewfs://ns/tmp/x"))
    assert(VegamPaths.isSupported("s3a://bucket/x"))
    assert(VegamPaths.isSupported("/tmp/x"))
    assert(!VegamPaths.isSupported(""))
  }

  test("clean strips file: prefix and keeps a leading slash") {
    assert(VegamPaths.clean("file:/tmp/x") === "/tmp/x")
    assert(VegamPaths.clean("file:///tmp/x") === "/tmp/x")
    assert(VegamPaths.clean("/tmp/x") === "/tmp/x")
    assert(VegamPaths.clean("hdfs://nn/tmp/x") === "hdfs://nn/tmp/x")
  }
}

class NativePlanCodecSuite extends org.apache.spark.SparkFunSuite {
  test("round-trip CountStar and HashAgg") {
    val count = CountStar(Seq("/tmp/a.parquet", "/tmp/b.parquet"))
    assert(NativePlanCodec.decode(NativePlanCodec.encode(count)) === count)

    val agg = HashAgg(
      files = Seq("/tmp/a.parquet"),
      groupCol = "k",
      sumCol = "v",
      filter = None,
      sumScale = 0,
      groupType = org.apache.spark.sql.types.LongType,
      sumType = org.apache.spark.sql.types.DoubleType,
      complete = false)
    val back = NativePlanCodec.decode(NativePlanCodec.encode(agg)).asInstanceOf[HashAgg]
    assert(back.files === agg.files)
    assert(back.groupCol === agg.groupCol)
    assert(back.sumCol === agg.sumCol)
    assert(back.complete === agg.complete)
  }

  test("round-trip StagePlan expand") {
    val plan = org.apache.spark.sql.vegam.plan.StagePlan(
      probe = org.apache.spark.sql.vegam.plan.ScanSpec(
        Seq(org.apache.spark.sql.vegam.plan.FileRef("/tmp/a.parquet", Nil)), Seq("k", "v")),
      builds = Nil,
      probeFilters = Nil,
      groups = Seq("k", "gid"),
      groupTypes = Seq(org.apache.spark.sql.types.LongType, org.apache.spark.sql.types.IntegerType),
      aggs = Seq(org.apache.spark.sql.vegam.plan.AggCall(
        org.apache.spark.sql.vegam.plan.NativePlan.AGG_SUM, "v", 0,
        org.apache.spark.sql.types.DoubleType)),
      window = None,
      complete = false,
      expand = Some(org.apache.spark.sql.vegam.plan.ExpandSpec(
        Seq("k", "v", "gid"),
        Seq(
          Seq(
            org.apache.spark.sql.vegam.plan.ExpandSlot(
              org.apache.spark.sql.vegam.plan.NativePlan.EXPAND_COL, "k"),
            org.apache.spark.sql.vegam.plan.ExpandSlot(
              org.apache.spark.sql.vegam.plan.NativePlan.EXPAND_COL, "v"),
            org.apache.spark.sql.vegam.plan.ExpandSlot(
              org.apache.spark.sql.vegam.plan.NativePlan.EXPAND_LONG, lvalue = 0L)),
          Seq(
            org.apache.spark.sql.vegam.plan.ExpandSlot(
              org.apache.spark.sql.vegam.plan.NativePlan.EXPAND_NULL),
            org.apache.spark.sql.vegam.plan.ExpandSlot(
              org.apache.spark.sql.vegam.plan.NativePlan.EXPAND_COL, "v"),
            org.apache.spark.sql.vegam.plan.ExpandSlot(
              org.apache.spark.sql.vegam.plan.NativePlan.EXPAND_LONG, lvalue = 1L))))))
    val back = NativePlanCodec.decode(NativePlanCodec.encode(plan))
      .asInstanceOf[org.apache.spark.sql.vegam.plan.StagePlan]
    assert(back.expand.isDefined)
    assert(back.expand.get.projections.length === 2)
    assert(back.groups === Seq("k", "gid"))
  }
}

class VegamSuite extends SharedSparkSession {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.vegam.VegamExtensions")
  }

  private def withVegam[T](fn: => T): T = {
    withSQLConf(
        VegamConf.VEGAM_ENABLED.key -> "true",
        VegamConf.VEGAM_BACKEND.key -> "jvm",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      fn
    }
  }

  private def hasNative(plan: SparkPlan): Boolean = plan match {
    case a: AdaptiveSparkPlanExec =>
      hasNative(a.inputPlan) || hasNative(a.executedPlan)
    case other =>
      other.exists(_.isInstanceOf[NativeStageExec])
  }

  test("disabled by default") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(path)
      val df = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY k")
      assert(!hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
    }
  }

  test("group-sum and multi-filter AND are rewritten") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)

      withVegam {
        val ok = sql(s"SELECT k, SUM(v) FROM parquet.`$path` WHERE k > 0 GROUP BY k")
        assert(hasNative(ok.queryExecution.executedPlan),
          ok.queryExecution.executedPlan.toString)
        checkAnswer(ok, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))

        val residual = sql(
          s"SELECT k, SUM(v) FROM parquet.`$path` WHERE k > 0 AND v < 10 GROUP BY k")
        assert(hasNative(residual.queryExecution.executedPlan),
          residual.queryExecution.executedPlan.toString)
        checkAnswer(residual, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))
      }
    }
  }

  test("group-sum of decimal(7,2) matches Spark scale") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, BigDecimal("1.50")), (1L, BigDecimal("2.54")), (2L, BigDecimal("3.00")))
        .toDF("k", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT k, SUM(v) FROM parquet.`$path` WHERE k > 0 GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, BigDecimal("4.04")), (2L, BigDecimal("3.00"))).toDF("k", "sum(v)"))
      }
    }
  }

  test("COUNT(*) on a local file uses NativeStage") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT COUNT(*) FROM parquet.`$path`")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq(2L).toDF("count(1)"))
      }
    }
  }

  test("multi-agg count and sum") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.0), (1L, 3.0), (2L, 5.0)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT k, COUNT(*), SUM(v) FROM parquet.`$path` GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, 2L, 4.0), (2L, 1L, 5.0)).toDF("k", "count(1)", "sum(v)"))
      }
    }
  }

  test("broadcast join plus group-sum") {
    withTempPath { fact =>
      withTempPath { dim =>
        val fp = fact.getCanonicalPath
        val dp = dim.getCanonicalPath
        Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(fp)
        Seq((1L, "a"), (2L, "b")).toDF("k", "n").write.mode("overwrite").parquet(dp)
        withVegam {
          val df = sql(
            s"SELECT f.k, SUM(f.v) FROM parquet.`$fp` f " +
              s"JOIN parquet.`$dp` d ON f.k = d.k GROUP BY f.k")
          assert(hasNative(df.queryExecution.executedPlan),
            df.queryExecution.executedPlan.toString)
          checkAnswer(df, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))
        }
      }
    }
  }

  test("row_number window") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 10L), (1L, 20L), (2L, 5L)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(
          s"SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn " +
            s"FROM parquet.`$path`")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, 10L, 1), (1L, 20L, 2), (2L, 5L, 1))
          .toDF("k", "v", "rn"))
      }
    }
  }

  test("partitioned parquet injects partition column") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5, 2020), (2L, 3.0, 2020)).toDF("k", "v", "year")
        .write.mode("overwrite").partitionBy("year").parquet(path)
      withVegam {
        val df = sql(s"SELECT year, SUM(v) FROM parquet.`$path` GROUP BY year")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((2020, 4.5)).toDF("year", "sum(v)"))
      }
    }
  }

  test("exists and in become semi-join native stages") {
    withTempPath { fact =>
      withTempPath { dim =>
        val fp = fact.getCanonicalPath
        val dp = dim.getCanonicalPath
        Seq((1L, 1.5), (2L, 3.0), (3L, 9.0)).toDF("k", "v").write.mode("overwrite").parquet(fp)
        Seq(1L, 2L).toDF("k").write.mode("overwrite").parquet(dp)
        withVegam {
          val inn = sql(
            s"SELECT k, SUM(v) FROM parquet.`$fp` WHERE k IN (SELECT k FROM parquet.`$dp`) " +
              "GROUP BY k")
          assert(hasNative(inn.queryExecution.executedPlan),
            inn.queryExecution.executedPlan.toString)
          checkAnswer(inn, Seq((1L, 1.5), (2L, 3.0)).toDF("k", "sum(v)"))

          val ex = sql(
            s"SELECT f.k, SUM(f.v) FROM parquet.`$fp` f WHERE EXISTS (" +
              s"SELECT 1 FROM parquet.`$dp` d WHERE d.k = f.k) GROUP BY f.k")
          assert(hasNative(ex.queryExecution.executedPlan),
            ex.queryExecution.executedPlan.toString)
          checkAnswer(ex, Seq((1L, 1.5), (2L, 3.0)).toDF("k", "sum(v)"))
        }
      }
    }
  }

  test("union all of two parquet scans fuses") {
    withTempPath { a =>
      withTempPath { b =>
        val ap = a.getCanonicalPath
        val bp = b.getCanonicalPath
        Seq((1L, 1.0)).toDF("k", "v").write.mode("overwrite").parquet(ap)
        Seq((1L, 2.0), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(bp)
        withVegam {
          val df = sql(
            s"SELECT k, SUM(v) FROM (" +
              s"SELECT k, v FROM parquet.`$ap` UNION ALL SELECT k, v FROM parquet.`$bp`" +
              ") t GROUP BY k")
          assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
          checkAnswer(df, Seq((1L, 3.0), (2L, 3.0)).toDF("k", "sum(v)"))
        }
      }
    }
  }

  test("sort-merge join plus group-sum") {
    withTempPath { fact =>
      withTempPath { dim =>
        val fp = fact.getCanonicalPath
        val dp = dim.getCanonicalPath
        Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(fp)
        Seq((1L, "a"), (2L, "b")).toDF("k", "n").write.mode("overwrite").parquet(dp)
        withVegam {
          withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
            val df = sql(
              s"SELECT f.k, SUM(f.v) FROM parquet.`$fp` f " +
                s"JOIN parquet.`$dp` d ON f.k = d.k GROUP BY f.k")
            assert(hasNative(df.queryExecution.executedPlan),
              df.queryExecution.executedPlan.toString)
            checkAnswer(df, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))
          }
        }
      }
    }
  }

  test("rollup and grouping sets fuse expand") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withVegam {
        val roll = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY ROLLUP(k)")
        assert(hasNative(roll.queryExecution.executedPlan),
          roll.queryExecution.executedPlan.toString)
        checkAnswer(roll, Seq(Row(1L, 4.0), Row(2L, 3.0), Row(null, 7.0)))

        val gs = sql(
          s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY GROUPING SETS ((k), ())")
        assert(hasNative(gs.queryExecution.executedPlan),
          gs.queryExecution.executedPlan.toString)
        checkAnswer(gs, Seq(Row(1L, 4.0), Row(2L, 3.0), Row(null, 7.0)))
      }
    }
  }

  test("AQE on still rewrites group-sum") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withSQLConf(
          VegamConf.VEGAM_ENABLED.key -> "true",
          VegamConf.VEGAM_BACKEND.key -> "jvm",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        val df = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, 4.0)).toDF("k", "sum(v)"))
      }
    }
  }

  test("native HashAgg group-sum when libvegam is loaded") {
    if (!NativeTask.isLoaded) {
      cancel("libvegam not on java.library.path; this is the non-Velox native path")
    }
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)
      withSQLConf(
          VegamConf.VEGAM_ENABLED.key -> "true",
          VegamConf.VEGAM_BACKEND.key -> "native",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan),
          df.queryExecution.executedPlan.toString)
        assert(df.queryExecution.executedPlan.toString.contains("native"),
          df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))

        val joined = sql(
          s"SELECT k, SUM(v) FROM parquet.`$path` WHERE k IN (SELECT k FROM parquet.`$path`) " +
            "GROUP BY k")
        assert(hasNative(joined.queryExecution.executedPlan),
          joined.queryExecution.executedPlan.toString)
        checkAnswer(joined, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))

        val roll = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY ROLLUP(k)")
        assert(hasNative(roll.queryExecution.executedPlan),
          roll.queryExecution.executedPlan.toString)
        checkAnswer(roll, Seq(Row(1L, 4.0), Row(2L, 3.0), Row(null, 7.0)))
      }
    }
  }

  test("native backend without libvegam does not rewrite") {
    if (NativeTask.isLoaded) {
      cancel("libvegam is on the path; rewrite is expected")
    }
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withSQLConf(
        VegamConf.VEGAM_ENABLED.key -> "true",
        VegamConf.VEGAM_BACKEND.key -> "native") {
        val df = sql(s"SELECT COUNT(*) FROM parquet.`$path`")
        assert(!hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq(1L).toDF("count(1)"))
      }
    }
  }
}
