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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
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
}

class VegamSuite extends SharedSparkSession {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.vegam.VegamExtensions")
  }

  private def withVegam[T](fn: => T): T = {
    withSQLConf(
      VegamConf.VEGAM_ENABLED.key -> "true",
      VegamConf.VEGAM_BACKEND.key -> "jvm") {
      fn
    }
  }

  private def hasNative(plan: SparkPlan): Boolean = {
    plan.exists(_.isInstanceOf[NativeStageExec])
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

  test("native backend without libvegam does not rewrite") {
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
