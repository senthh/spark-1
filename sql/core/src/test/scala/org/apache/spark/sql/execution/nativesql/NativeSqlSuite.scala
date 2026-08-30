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

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class NativeSqlSuite extends SharedSparkSession {

  import testImplicits._

  private def nativeLib: Option[String] = {
    val roots = Seq(
      new File("sql/native-cpp/build"),
      new File("../native-cpp/build"),
      new File("../../sql/native-cpp/build"))
    val names = Seq("libspark_nativesql_jni.dylib", "libspark_nativesql_jni.so")
    roots.flatMap(r => names.map(n => new File(r, n))).find(_.isFile).map(_.getAbsolutePath)
  }

  private def withNative[T](f: => T): T = {
    val lib = nativeLib.getOrElse {
      cancel("Build sql/native-cpp first: sql/native-cpp/build.sh")
      ""
    }
    withSQLConf(
        SQLConf.NATIVE_SQL_ENABLED.key -> "true",
        SQLConf.NATIVE_SQL_LIB.key -> lib,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      f
    }
  }

  test("disabled by default") {
    assert(!SQLConf.get.nativeSqlEnabled)
    val df = Seq((1, 10L), (2, 20L)).toDF("id", "v").filter($"id" > 1)
    assert(df.queryExecution.executedPlan.collect { case n: NativeSqlExec => n }.isEmpty)
  }

  test("filter + project offload") {
    withNative {
      val df = Seq((1, 10L), (2, 20L), (3, 30L)).toDF("id", "v")
        .filter($"id" > 1)
        .select($"id", ($"id" + $"v").as("s"))
      assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[NativeSqlExec]))
      checkAnswer(df, Seq(org.apache.spark.sql.Row(2, 22L), org.apache.spark.sql.Row(3, 33L)))
    }
  }

  test("hash aggregate offload") {
    withNative {
      val df = Seq((1, 10L), (1, 15L), (2, 20L)).toDF("k", "v")
        .groupBy($"k").agg(sum($"v").as("s"))
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[NativeSqlExec]))
      checkAnswer(df, Seq(org.apache.spark.sql.Row(1, 25L), org.apache.spark.sql.Row(2, 20L)))
    }
  }

  test("hash join offload") {
    withNative {
      val a = Seq((1, 10L), (2, 20L), (3, 30L)).toDF("id", "av")
      val b = Seq((2, 200L), (3, 300L)).toDF("id", "bv")
      val df = a.join(b, a("id") === b("id"))
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[NativeSqlExec]))
      checkAnswer(
        df.select(a("id"), $"av", $"bv"),
        Seq(org.apache.spark.sql.Row(2, 20L, 200L), org.apache.spark.sql.Row(3, 30L, 300L)))
    }
  }

  test("unsupported strings fall back") {
    withNative {
      val df = Seq("a", "b").toDF("s").filter($"s" === "a")
      assert(df.queryExecution.sparkPlan.collect { case n: NativeSqlExec => n }.isEmpty)
      checkAnswer(df, Seq(org.apache.spark.sql.Row("a")))
    }
  }

  test("sort compiles to IR") {
    val df = Seq((3, 30), (1, 10), (2, 20)).toDF("id", "v").orderBy($"id")
    val compiled = NativeSqlPlan.compile(df.queryExecution.optimizedPlan)
    assert(compiled.isDefined)
    // Example: (sort (list c0) (scan 0))
    assert(compiled.get.ir.startsWith("(sort (list c0)"))
    assert(!df.queryExecution.optimizedPlan.exists(NativeSqlPlan.isFileScanLeaf))
  }

  test("sort offload") {
    withNative {
      val df = Seq((3, 30), (1, 10), (2, 20)).toDF("id", "v").orderBy($"id")
      val compiled = NativeSqlPlan.compile(df.queryExecution.optimizedPlan)
      assert(compiled.isDefined)
      assert(compiled.get.ir.startsWith("(sort (list c0)"))
      assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[NativeSqlExec]))
      checkAnswer(
        df,
        Seq(
          org.apache.spark.sql.Row(1, 10),
          org.apache.spark.sql.Row(2, 20),
          org.apache.spark.sql.Row(3, 30)))
    }
  }

  test("parquet int filter compiles to file-backed IR") {
    withTempPath { dir =>
      Seq((1, 10), (2, 20), (3, 30)).toDF("id", "v").write.parquet(dir.getCanonicalPath)
      withSQLConf(
          SQLConf.NATIVE_SQL_ENABLED.key -> "true",
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val df = spark.read.parquet(dir.getCanonicalPath).filter($"id" > 1)
        assert(df.queryExecution.optimizedPlan.exists(NativeSqlPlan.isFileScanLeaf))
        val compiled = NativeSqlPlan.compile(df.queryExecution.optimizedPlan)
        assert(compiled.isDefined)
        assert(compiled.get.hasFileLeaf)
        assert(compiled.get.ir.contains("(filter"))
        val withNull = spark.read.parquet(dir.getCanonicalPath)
          .filter($"id".isNotNull && $"id" > 1)
        val compiledNull = NativeSqlPlan.compile(withNull.queryExecution.optimizedPlan)
        assert(compiledNull.isDefined)
        assert(compiledNull.get.ir.contains("true") || compiledNull.get.ir.contains("(gt"))
      }
    }
  }

  test("parquet int filter offload") {
    withNative {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { dir =>
          Seq((1, 10), (2, 20), (3, 30)).toDF("id", "v").write.parquet(dir.getCanonicalPath)
          val df = spark.read.parquet(dir.getCanonicalPath).filter($"id" > 1)
          // Native parquet scan: C++ reads the files (Velox-style). Spark
          // still plans splits and keeps dataFilters for listing / skip.
          assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[NativeSqlExec]))
          val scans = df.queryExecution.executedPlan.collect {
            case s: org.apache.spark.sql.execution.FileSourceScanExec => s
          }
          assert(scans.nonEmpty && scans.head.dataFilters.nonEmpty)
          checkAnswer(
            df.select($"id", $"v"),
            Seq(org.apache.spark.sql.Row(2, 20), org.apache.spark.sql.Row(3, 30)))
        }
      }
    }
  }

  test("parquet filter keeps FileSource dataFilters") {
    withNative {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { dir =>
          Seq((1, 10), (2, 20), (3, 30)).toDF("id", "v").write.parquet(dir.getCanonicalPath)
          val df = spark.read.parquet(dir.getCanonicalPath).filter($"id" > 1)
          val scans = df.queryExecution.executedPlan.collect {
            case s: org.apache.spark.sql.execution.FileSourceScanExec => s
          }
          assert(scans.nonEmpty)
          assert(scans.head.dataFilters.nonEmpty,
            s"expected Parquet pushdown, dataFilters=${scans.head.dataFilters}")
        }
      }
    }
  }

  test("parquet inner join offload") {
    withNative {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { dir =>
          val aPath = new File(dir, "a").getCanonicalPath
          val bPath = new File(dir, "b").getCanonicalPath
          Seq((1, 10), (2, 20), (3, 30)).toDF("id", "av").write.parquet(aPath)
          Seq((2, 200), (3, 300)).toDF("id", "bv").write.parquet(bPath)
          val a = spark.read.parquet(aPath)
          val b = spark.read.parquet(bPath)
          val df = a.join(b, a("id") === b("id"))
          assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[NativeSqlExec]))
          checkAnswer(
            df.select(a("id"), $"av", $"bv"),
            Seq(org.apache.spark.sql.Row(2, 20, 200), org.apache.spark.sql.Row(3, 30, 300)))
        }
      }
    }
  }

  test("parquet 3-way join plus hash agg offload") {
    withNative {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { dir =>
          val fact = new File(dir, "fact").getCanonicalPath
          val d = new File(dir, "d").getCanonicalPath
          val i = new File(dir, "i").getCanonicalPath
          Seq((10, 1, 100L), (11, 1, 50L), (10, 2, 7L)).toDF("dk", "ik", "amt")
            .write.parquet(fact)
          Seq((10, 2000), (11, 2001)).toDF("dk", "yr").write.parquet(d)
          Seq((1, "A"), (2, "B")).toDF("ik", "brand").write.parquet(i)
          val f = spark.read.parquet(fact)
          val dd = spark.read.parquet(d)
          val ii = spark.read.parquet(i)
          val df = f.join(dd, f("dk") === dd("dk"))
            .join(ii, f("ik") === ii("ik"))
            .groupBy($"yr", $"brand")
            .agg(sum($"amt").as("s"))
          assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[NativeSqlExec]),
            df.queryExecution.sparkPlan.toString)
          checkAnswer(
            df,
            Seq(
              org.apache.spark.sql.Row(2000, "A", 100L),
              org.apache.spark.sql.Row(2001, "A", 50L),
              org.apache.spark.sql.Row(2000, "B", 7L)))
        }
      }
    }
  }

  test("parquet join gathers string payload") {
    withNative {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        withTempPath { dir =>
          val aPath = new File(dir, "a").getCanonicalPath
          val bPath = new File(dir, "b").getCanonicalPath
          Seq((1, "x"), (2, "y")).toDF("id", "name").write.parquet(aPath)
          Seq((2, 200), (3, 300)).toDF("id", "bv").write.parquet(bPath)
          val a = spark.read.parquet(aPath)
          val b = spark.read.parquet(bPath)
          val df = a.join(b, a("id") === b("id"))
          assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[NativeSqlExec]))
          checkAnswer(df.select($"name", $"bv"), Seq(org.apache.spark.sql.Row("y", 200)))
        }
      }
    }
  }
}
