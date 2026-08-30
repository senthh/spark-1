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
    val root = new File("sql/native-cpp/build")
    val candidates = Seq(
      new File(root, "libspark_nativesql_jni.dylib"),
      new File(root, "libspark_nativesql_jni.so"))
    candidates.find(_.isFile).map(_.getAbsolutePath)
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

  test("parquet file scan is not swallowed as leaf native exec") {
    withSQLConf(
        SQLConf.NATIVE_SQL_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir =>
        Seq((1, 10), (2, 20), (3, 30)).toDF("id", "v").write.parquet(dir.getCanonicalPath)
        val df = spark.read.parquet(dir.getCanonicalPath).filter($"id" > 1)
        assert(df.queryExecution.optimizedPlan.exists(NativeSqlPlan.isFileScanLeaf))
        assert(NativeSqlPlan.compile(df.queryExecution.optimizedPlan).isEmpty)
        val native = df.queryExecution.executedPlan.collect { case n: NativeSqlExec => n }
        assert(native.isEmpty, "FileScan must not be compiled as NativeSqlExec")
      }
    }
  }
}
