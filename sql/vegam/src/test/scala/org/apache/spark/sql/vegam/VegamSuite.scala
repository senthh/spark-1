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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, LongType}
import org.apache.spark.sql.vegam.plan.{AggCall, BuildJoin, CountStar, FileRef, HashAgg,
  NativePlan, NativePlanCodec, ScanSpec, StagePlan}

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

class VegamBackendSuite extends org.apache.spark.SparkFunSuite {
  import org.apache.spark.sql.vegam.exec.VegamBackend
  import org.apache.spark.sql.vegam.exec.VegamBackend.{AUTO, JVM, NATIVE}

  private def stage(window: Boolean): StagePlan = StagePlan(
    probe = ScanSpec(Seq(FileRef("/tmp/a.parquet", Nil)), Seq("k", "v"), Seq("bigint", "bigint")),
    builds = Nil,
    probeFilters = Nil,
    groups = Seq("k"),
    groupTypes = Seq(LongType),
    aggs = Seq(AggCall(NativePlan.AGG_SUM, "v", 0, LongType)),
    window = if (window) {
      Some(org.apache.spark.sql.vegam.plan.WinSpec(Seq("k"), Nil, Nil))
    } else {
      None
    },
    complete = false)

  test("auto runs what libvegam supports and leaves the rest on Spark") {
    assert(!stage(window = false).jvmOnly)
    assert(stage(window = true).jvmOnly)
    assert(VegamBackend.pick(AUTO, stage(window = false), loaded = true) === Some(NATIVE))
    assert(VegamBackend.pick(AUTO, stage(window = true), loaded = true) === None)
    assert(VegamBackend.pick(AUTO, stage(window = false), loaded = false) === None)
    assert(VegamBackend.resolve(AUTO, loaded = false) === None)
  }

  test("stages Velox rejects are not sent to libvegam") {
    val base = stage(window = false)
    assert(base.copy(probe = base.probe.copy(types = Nil)).jvmOnly)
    assert(base.copy(complete = true).jvmOnly)
    assert(base.copy(groups = Nil, groupTypes = Nil, aggs = Nil).jvmOnly)
    val build = BuildJoin(
      scan = ScanSpec(Seq(FileRef("/tmp/d.parquet", Nil)), Seq("k"), Seq("bigint")),
      probeKeys = Seq("k"),
      buildKeys = Seq("k"),
      joinType = NativePlan.JOIN_INNER,
      filters = Nil)
    assert(!base.copy(builds = Seq(build)).jvmOnly)
    assert(base.copy(builds = Seq(build.copy(broadcast = false))).jvmOnly)
  }

  test("jvm is only used when requested") {
    assert(VegamBackend.pick(JVM, stage(window = true), loaded = true) === Some(JVM))
    assert(VegamBackend.pick(JVM, stage(window = false), loaded = false) === Some(JVM))
    assert(VegamBackend.pick(NATIVE, stage(window = true), loaded = true) === None)
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

  test("round-trip StagePlan file ranges, scan types, build mode and agg input") {
    val plan = StagePlan(
      probe = ScanSpec(
        Seq(FileRef("/tmp/a.parquet", Seq("d" -> "1"), 0L, 128L),
          FileRef("/tmp/a.parquet", Seq("d" -> "1"), 128L, 64L)),
        Seq("k", "v"),
        Seq("bigint", "decimal(7,2)")),
      builds = Seq(BuildJoin(
        ScanSpec(Seq(FileRef("/tmp/b.parquet", Nil)), Seq("bk"), Seq("bigint")),
        probeKeys = Seq("k"),
        buildKeys = Seq("bk"),
        joinType = NativePlan.JOIN_INNER,
        filters = Nil,
        broadcast = false)),
      probeFilters = Nil,
      groups = Seq("k"),
      groupTypes = Seq(LongType),
      aggs = Seq(AggCall(NativePlan.AGG_SUM, "v", 2, DecimalType(17, 2),
        NativePlan.INPUT_UNSCALED)),
      window = None,
      complete = false)
    assert(NativePlanCodec.decode(NativePlanCodec.encode(plan)) === plan)
  }

  test("wholeFiles keeps one whole-file ref per file") {
    val refs = Seq(
      FileRef("/tmp/a.parquet", Nil, 0L, 100L),
      FileRef("/tmp/a.parquet", Nil, 100L, 100L),
      FileRef("/tmp/b.parquet", Nil, 0L, 50L))
    assert(FileRef.wholeFiles(refs) ===
      Seq(FileRef("/tmp/a.parquet", Nil), FileRef("/tmp/b.parquet", Nil)))
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

  test("dim filters above a join stay on the dim") {
    withTempPath { fact =>
      withTempPath { date =>
        withTempPath { item =>
          val fp = fact.getCanonicalPath
          val dp = date.getCanonicalPath
          val ip = item.getCanonicalPath
          Seq((1L, 10L, 1.5), (1L, 20L, 2.5), (2L, 10L, 9.0))
            .toDF("date_sk", "item_sk", "price").write.mode("overwrite").parquet(fp)
          Seq((1L, 11, 2001), (2L, 12, 2001))
            .toDF("date_sk", "moy", "year").write.mode("overwrite").parquet(dp)
          Seq((10L, 128, "b1"), (20L, 129, "b2"))
            .toDF("item_sk", "manufact_id", "brand").write.mode("overwrite").parquet(ip)
          withVegam {
            val df = sql(
              s"SELECT d.year, i.brand, SUM(f.price) FROM parquet.`$fp` f " +
                s"JOIN parquet.`$dp` d ON f.date_sk = d.date_sk " +
                s"JOIN parquet.`$ip` i ON f.item_sk = i.item_sk " +
                "WHERE i.manufact_id = 128 AND d.moy = 11 " +
                "GROUP BY d.year, i.brand")
            assert(hasNative(df.queryExecution.executedPlan),
              df.queryExecution.executedPlan.toString)
            checkAnswer(df, Seq((2001, "b1", 1.5)).toDF("year", "brand", "sum(price)"))
          }
        }
      }
    }
  }

  test("the same dim joined twice stays on Spark") {
    withTempPath { fact =>
      withTempPath { dim =>
        val fp = fact.getCanonicalPath
        val dp = dim.getCanonicalPath
        Seq((1L, 2L, 1.5), (2L, 1L, 2.5)).toDF("a", "b", "v").write.parquet(fp)
        Seq((1L, "x"), (2L, "y")).toDF("sk", "name").write.parquet(dp)
        withVegam {
          val df = sql(
            s"SELECT d1.name, d2.name, SUM(f.v) FROM parquet.`$fp` f " +
              s"JOIN parquet.`$dp` d1 ON f.a = d1.sk " +
              s"JOIN parquet.`$dp` d2 ON f.b = d2.sk " +
              "GROUP BY d1.name, d2.name")
          val plan = df.queryExecution.executedPlan
          val fused = plan.exists {
            case n: NativeStageExec =>
              n.nativePlan match {
                case s: StagePlan => s.builds.length >= 2
                case _ => false
              }
            case _ => false
          }
          assert(!fused, plan.toString)
          checkAnswer(df, Seq(Row("x", "y", 1.5), Row("y", "x", 2.5)))
        }
      }
    }
  }

  test("a file split into many ranges is read once") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 20000).selectExpr("id % 7 AS k", "CAST(id AS DOUBLE) AS v")
        .coalesce(1).write.parquet(path)
      withVegam {
        withSQLConf(
            SQLConf.FILES_MAX_PARTITION_BYTES.key -> "4096",
            SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "0") {
          val df = sql(s"SELECT k, COUNT(*), SUM(v) FROM parquet.`$path` GROUP BY k")
          assert(hasNative(df.queryExecution.executedPlan),
            df.queryExecution.executedPlan.toString)
          val expected = (0L until 20000L).groupBy(_ % 7).map { case (k, ids) =>
            Row(k, ids.length.toLong, ids.sum.toDouble)
          }.toSeq
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("distinct over a join is not collapsed") {
    withTempPath { fact =>
      withTempPath { dim =>
        val fp = fact.getCanonicalPath
        val dp = dim.getCanonicalPath
        Seq((1L, 10L), (1L, 10L), (2L, 11L), (3L, 10L))
          .toDF("k", "id").write.parquet(fp)
        Seq(
          (10L, "ann", java.sql.Date.valueOf("1998-12-01")),
          (11L, "bob", java.sql.Date.valueOf("1998-12-02")))
          .toDF("id", "name", "d").write.parquet(dp)
        withVegam {
          val df = sql(
            s"SELECT DISTINCT name, d FROM parquet.`$fp` f " +
              s"JOIN parquet.`$dp` d ON f.id = d.id")
          assert(hasNative(df.queryExecution.executedPlan),
            df.queryExecution.executedPlan.toString)
          checkAnswer(df, Seq(
            Row("ann", java.sql.Date.valueOf("1998-12-01")),
            Row("bob", java.sql.Date.valueOf("1998-12-02"))))
        }
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

  test("exists and in semi-joins stay on Spark") {
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
          assert(!hasNative(inn.queryExecution.executedPlan),
            inn.queryExecution.executedPlan.toString)
          checkAnswer(inn, Seq((1L, 1.5), (2L, 3.0)).toDF("k", "sum(v)"))

          val ex = sql(
            s"SELECT f.k, SUM(f.v) FROM parquet.`$fp` f WHERE EXISTS (" +
              s"SELECT 1 FROM parquet.`$dp` d WHERE d.k = f.k) GROUP BY f.k")
          assert(!hasNative(ex.queryExecution.executedPlan),
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
        checkVegamMatches(
          s"SELECT k, SUM(v) FROM (" +
            s"SELECT k, v FROM parquet.`$ap` UNION ALL SELECT k, v FROM parquet.`$bp`" +
            ") t GROUP BY k")
      }
    }
  }

  test("sort-merge join plus group-sum stays on Spark") {
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
            assert(!hasNative(df.queryExecution.executedPlan),
              df.queryExecution.executedPlan.toString)
            checkAnswer(df, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))
          }
        }
      }
    }
  }

  test("rollup and grouping sets stay on Spark") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v").write.mode("overwrite").parquet(path)
      withVegam {
        val roll = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY ROLLUP(k)")
        assert(!hasNative(roll.queryExecution.executedPlan),
          roll.queryExecution.executedPlan.toString)
        checkAnswer(roll, Seq(Row(1L, 4.0), Row(2L, 3.0), Row(null, 7.0)))

        val gs = sql(
          s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY GROUPING SETS ((k), ())")
        assert(!hasNative(gs.queryExecution.executedPlan),
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
        val plan = df.queryExecution.executedPlan
        assert(nativeCount(plan) == 1, plan.toString)
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

  private def nativeCount(plan: SparkPlan): Int = plan match {
    case a: AdaptiveSparkPlanExec =>
      math.max(nativeCount(a.inputPlan), nativeCount(a.executedPlan))
    case other => other.collect { case _: NativeStageExec => 1 }.sum
  }

  test("partial average matches Spark") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.0), (1L, 3.0), (2L, 5.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT k, AVG(v) FROM parquet.`$path` GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, 2.0), (2L, 5.0)).toDF("k", "avg(v)"))
      }
    }
  }

  test("sum of an arithmetic expression is native") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 5L, 2L), (1L, 4L, 1L), (2L, 9L, 3L)).toDF("k", "a", "b")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT k, SUM(a - b), SUM(a * b) FROM parquet.`$path` GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq(Row(1L, 6L, 14L), Row(2L, 6L, 27L)))
      }
    }
  }

  test("column equality and OR filters stay native") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1L, 10.0), (2L, 3L, 4.0), (3L, 3L, 5.0)).toDF("k", "a", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val eq = sql(s"SELECT SUM(v) FROM parquet.`$path` WHERE k = a")
        assert(hasNative(eq.queryExecution.executedPlan), eq.queryExecution.executedPlan.toString)
        checkAnswer(eq, Seq(15.0).toDF("sum(v)"))
        val either = sql(s"SELECT SUM(v) FROM parquet.`$path` WHERE k = 1 OR k = 2")
        assert(hasNative(either.queryExecution.executedPlan),
          either.queryExecution.executedPlan.toString)
        checkAnswer(either, Seq(14.0).toDF("sum(v)"))
      }
    }
  }

  test("final aggregate over the shuffle stays on Spark") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY k")
        val plan = df.queryExecution.executedPlan
        assert(nativeCount(plan) == 1, plan.toString)
        checkAnswer(df, Seq((1L, 4.0), (2L, 3.0)).toDF("k", "sum(v)"))
      }
    }
  }

  test("decimal sum over the shuffle keeps scale and column order") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq(
        (1L, "a", BigDecimal("1.50")),
        (1L, "a", BigDecimal("2.54")),
        (2L, "b", BigDecimal("3.00")))
        .toDF("k", "name", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(
          s"SELECT SUM(v), name, k FROM parquet.`$path` GROUP BY name, k")
        val plan = df.queryExecution.executedPlan
        assert(nativeCount(plan) == 1, plan.toString)
        checkAnswer(df, Seq(
          (BigDecimal("4.04"), "a", 1L),
          (BigDecimal("3.00"), "b", 2L)).toDF("sum(v)", "name", "k"))
      }
    }
  }

  test("tinyint group key round-trips through a native stage") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1.toByte, 1.5), (1.toByte, 2.5), (2.toByte, 3.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(s"SELECT k, SUM(v) FROM parquet.`$path` GROUP BY k")
        assert(hasNative(df.queryExecution.executedPlan),
          df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1.toByte, 4.0), (2.toByte, 3.0)).toDF("k", "sum(v)"))
      }
    }
  }

  test("running sum window over ordered rows") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1L, 10.0), (1L, 2L, 5.0), (2L, 1L, 7.0)).toDF("k", "d", "v")
        .write.mode("overwrite").parquet(path)
      withVegam {
        val df = sql(
          "SELECT k, d, SUM(v) OVER (PARTITION BY k ORDER BY d " +
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c " +
            s"FROM parquet.`$path`")
        assert(hasNative(df.queryExecution.executedPlan), df.queryExecution.executedPlan.toString)
        checkAnswer(df, Seq((1L, 1L, 10.0), (1L, 2L, 15.0), (2L, 1L, 7.0)).toDF("k", "d", "c"))
      }
    }
  }

  test("q1 shape: average times a literal stays scaled") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1, 1, 10.0), (1, 1, 10.0), (1, 2, 1.0), (2, 3, 5.0))
        .toDF("store", "cust", "fee").write.mode("overwrite").parquet(path)
      val inner =
        "SELECT cust, store, sum(fee) total FROM parquet.`" + path + "` GROUP BY cust, store"
      val q =
        "SELECT c.cust, c.total FROM (" + inner + ") c JOIN (" +
          "SELECT store, avg(total) * 1.2 lim FROM (" + inner + ") GROUP BY store) a " +
          "ON c.store = a.store WHERE c.total > a.lim"
      val expected = sql(q).collect().toSeq
      withVegam {
        val df = sql(q)
        checkAnswer(df, expected)
      }
    }
  }

  test("substr join key stays on Spark") {
    withTempPath { dir =>
      val base = dir.getCanonicalPath
      val sales = base + "/sales"
      val zips = base + "/zips"
      Seq((1L, "89436", 2.0), (1L, "99999", 8.0), (2L, "30868", 4.0))
        .toDF("k", "code", "v").write.mode("overwrite").parquet(sales)
      Seq(("89436-1234", "keep"), ("30868-0000", "keep"))
        .toDF("zip", "tag").write.mode("overwrite").parquet(zips)
      val q =
        "SELECT k, sum(v) s FROM parquet.`" + sales + "` s, parquet.`" + zips + "` z " +
          "WHERE s.code = substr(z.zip, 1, 5) GROUP BY k"
      val expected = sql(q).collect().toSeq
      withVegam {
        val df = sql(q)
        assert(!hasNative(df.queryExecution.executedPlan),
          df.queryExecution.executedPlan.toString)
        checkAnswer(df, expected)
      }
    }
  }

  test("q65 shape: decimal sum through a date join then average") {
    withTempPath { dir =>
      val base = dir.getCanonicalPath
      val sales = base + "/sales"
      val dates = base + "/dates"
      val stores = base + "/stores"
      val items = base + "/items"
      Seq(
        (1, 1, 10, BigDecimal("0.40")),
        (1, 1, 10, BigDecimal("0.49")),
        (1, 1, 20, BigDecimal("1.84")),
        (1, 2, 11, BigDecimal("10.00")),
        (2, 3, 10, BigDecimal("0.30")))
        .toDF("store_sk", "item_sk", "date_sk", "price")
        .withColumn("price", col("price").cast(DecimalType(7, 2)))
        .write.mode("overwrite").parquet(sales)
      Seq((10, 5), (11, 6), (20, 9)).toDF("date_sk", "month")
        .write.mode("overwrite").parquet(dates)
      Seq((1, "s1"), (2, "s2")).toDF("store_sk", "name")
        .write.mode("overwrite").parquet(stores)
      Seq((1, "d1"), (2, "d2"), (3, "d3")).toDF("item_sk", "descr")
        .write.mode("overwrite").parquet(items)
      val q =
        "SELECT st.name, it.descr, sc.revenue " +
          "FROM parquet.`" + stores + "` st, parquet.`" + items + "` it, " +
          "(SELECT store_sk, avg(revenue) ave FROM (" +
          "SELECT store_sk, item_sk, sum(price) revenue " +
          "FROM parquet.`" + sales + "` sa, parquet.`" + dates + "` d " +
          "WHERE sa.date_sk = d.date_sk AND d.month BETWEEN 5 AND 6 " +
          "GROUP BY store_sk, item_sk) x GROUP BY store_sk) sb, " +
          "(SELECT store_sk, item_sk, sum(price) revenue " +
          "FROM parquet.`" + sales + "` scs, parquet.`" + dates + "` d2 " +
          "WHERE scs.date_sk = d2.date_sk AND d2.month BETWEEN 5 AND 6 " +
          "GROUP BY store_sk, item_sk) sc " +
          "WHERE sb.store_sk = sc.store_sk AND sc.revenue <= 0.1 * sb.ave " +
          "AND st.store_sk = sc.store_sk AND it.item_sk = sc.item_sk"
      checkVegamMatches(q)
    }
  }

  test("q70 shape: rollup rank over a filtered join") {
    withTempPath { dir =>
      val base = dir.getCanonicalPath
      val sales = base + "/sales"
      val stores = base + "/stores"
      Seq(
        (1, 5, BigDecimal("4.00")),
        (1, 9, BigDecimal("9.00")),
        (2, 5, BigDecimal("1.00")),
        (2, 6, BigDecimal("3.00")),
        (3, 5, BigDecimal("8.00")))
        .toDF("store_sk", "month", "profit")
        .withColumn("profit", col("profit").cast(DecimalType(7, 2)))
        .write.mode("overwrite").parquet(sales)
      Seq((1, "TN", "a"), (2, "TN", "b"), (3, "AL", "c"))
        .toDF("store_sk", "state", "county")
        .write.mode("overwrite").parquet(stores)
      val q =
        "SELECT sum(profit) total_sum, state, county, " +
          "grouping(state) + grouping(county) loch, " +
          "rank() OVER (PARTITION BY grouping(state) + grouping(county), " +
          "CASE WHEN grouping(county) = 0 THEN state END " +
          "ORDER BY sum(profit) DESC) rk " +
          "FROM parquet.`" + sales + "` sa, parquet.`" + stores + "` st " +
          "WHERE sa.store_sk = st.store_sk AND sa.month BETWEEN 5 AND 6 " +
          "GROUP BY ROLLUP(state, county)"
      checkVegamMatches(q)
    }
  }

  test("q49 shape: rank over a wide decimal ratio keeps every digit") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 53L, 96L), (2L, 1L, 3L), (3L, 2L, 7L))
        .toDF("item", "ret", "qty").write.mode("overwrite").parquet(path)
      val q =
        "SELECT item, ratio, rank() OVER (ORDER BY ratio) rk FROM (" +
          "SELECT item, cast(sum(ret) as decimal(15,4)) / " +
          "cast(sum(qty) as decimal(15,4)) ratio " +
          "FROM parquet.`" + path + "` GROUP BY item) t"
      checkVegamMatches(q)
      withVegam {
        val plan = sql(q).queryExecution.executedPlan
        assert(nativeStages(plan).forall(_.window.isEmpty), plan.toString)
      }
    }
  }

  test("q86 shape: rollup of string keys from a joined dimension") {
    withTempPath { dir =>
      val base = dir.getCanonicalPath
      val sales = base + "/sales"
      val dates = base + "/dates"
      val items = base + "/items"
      Seq(
        (1, 10, BigDecimal("4.00")),
        (2, 10, BigDecimal("1.00")),
        (3, 11, BigDecimal("9.00")),
        (1, 12, BigDecimal("3.00")))
        .toDF("item_sk", "date_sk", "paid")
        .withColumn("paid", col("paid").cast(DecimalType(7, 2)))
        .write.mode("overwrite").parquet(sales)
      Seq((10, 5), (11, 5), (12, 9)).toDF("date_sk", "month")
        .write.mode("overwrite").parquet(dates)
      Seq((1, "home", "a"), (2, "home", "b"), (3, "books", "c"))
        .toDF("item_sk", "category", "klass")
        .write.mode("overwrite").parquet(items)
      val q =
        "SELECT sum(paid) total_sum, category, klass, " +
          "grouping(category) + grouping(klass) loch, " +
          "rank() OVER (PARTITION BY grouping(category) + grouping(klass), " +
          "CASE WHEN grouping(klass) = 0 THEN category END " +
          "ORDER BY sum(paid) DESC) rk " +
          "FROM parquet.`" + sales + "` sa, parquet.`" + dates + "` d, " +
          "parquet.`" + items + "` it " +
          "WHERE sa.date_sk = d.date_sk AND sa.item_sk = it.item_sk " +
          "AND d.month BETWEEN 5 AND 6 " +
          "GROUP BY ROLLUP(category, klass)"
      checkVegamMatches(q)
      withVegam {
        val plan = sql(q).queryExecution.executedPlan
        val stages = nativeStages(plan)
        assert(stages.forall(_.expand.isEmpty), plan.toString)
      }
    }
  }

  test("q23 shape: semi-join filter then sum of a product") {
    withTempPath { dir =>
      val base = dir.getCanonicalPath
      val sales = base + "/sales"
      val dates = base + "/dates"
      val items = base + "/items"
      Seq(
        (1, 1, 10, 2, BigDecimal("3.00")),
        (2, 1, 10, 1, BigDecimal("4.00")),
        (3, 9, 10, 5, BigDecimal("8.00")),
        (1, 1, 11, 7, BigDecimal("1.00")))
        .toDF("item_sk", "cust", "date_sk", "qty", "price")
        .withColumn("price", col("price").cast(DecimalType(7, 2)))
        .write.mode("overwrite").parquet(sales)
      Seq((10, 1999, 1), (11, 2001, 1)).toDF("date_sk", "year", "moy")
        .write.mode("overwrite").parquet(dates)
      Seq((1, "red shirt"), (9, "blue hat")).toDF("item_sk", "descr")
        .write.mode("overwrite").parquet(items)
      val q =
        "SELECT sum(qty * price) s FROM parquet.`" + sales + "` sa, " +
          "parquet.`" + dates + "` d " +
          "WHERE sa.date_sk = d.date_sk AND d.year = 1999 AND d.moy = 1 " +
          "AND sa.item_sk IN (SELECT item_sk FROM parquet.`" + items + "` " +
          "WHERE descr = 'red shirt') " +
          "AND sa.cust IN (SELECT cust FROM parquet.`" + sales + "` " +
          "GROUP BY cust HAVING sum(qty) > 2)"
      checkVegamMatches(q)
    }
  }

  test("q56 shape: color membership across a union of sums") {
    withTempPath { dir =>
      val base = dir.getCanonicalPath
      val sales = base + "/sales"
      val items = base + "/items"
      val addr = base + "/addr"
      Seq((1, 1, BigDecimal("5.00")), (2, 1, BigDecimal("1.00")), (3, 2, BigDecimal("9.00")))
        .toDF("item_sk", "addr_sk", "amt")
        .withColumn("amt", col("amt").cast(DecimalType(7, 2)))
        .write.mode("overwrite").parquet(sales)
      Seq((1, "A-OK", "orchid"), (2, "A-OL", "chiffon"), (3, "A-NO", "black"))
        .toDF("item_sk", "item_id", "color")
        .write.mode("overwrite").parquet(items)
      Seq((1, -8), (2, 0)).toDF("addr_sk", "gmt").write.mode("overwrite").parquet(addr)
      val q =
        "SELECT item_id, sum(amt) total_sales FROM (" +
          "SELECT it.item_id, sa.amt FROM parquet.`" + sales + "` sa, " +
          "parquet.`" + items + "` it, parquet.`" + addr + "` ca " +
          "WHERE sa.item_sk = it.item_sk AND sa.addr_sk = ca.addr_sk " +
          "AND ca.gmt = -8 AND it.color IN ('orchid', 'chiffon')" +
          ") t GROUP BY item_id ORDER BY item_id"
      checkVegamMatches(q)
    }
  }

  private def nativeStages(plan: SparkPlan): Seq[StagePlan] = {
    val found = scala.collection.mutable.ArrayBuffer.empty[StagePlan]
    def walk(p: SparkPlan): Unit = p match {
      case a: AdaptiveSparkPlanExec =>
        walk(a.inputPlan)
        walk(a.executedPlan)
      case n: NativeStageExec =>
        n.nativePlan match {
          case s: StagePlan => found += s
          case _ =>
        }
        n.children.foreach(walk)
      case other =>
        other.children.foreach(walk)
    }
    walk(plan)
    found.toSeq
  }

  private def checkVegamMatches(q: String): Unit = {
    val expected = sql(q).collect().toSeq
    withVegam {
      val df = sql(q)
      checkAnswer(df, expected)
    }
    withSQLConf(
        VegamConf.VEGAM_ENABLED.key -> "true",
        VegamConf.VEGAM_BACKEND.key -> "jvm",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      val df = sql(q)
      checkAnswer(df, expected)
    }
  }
}
