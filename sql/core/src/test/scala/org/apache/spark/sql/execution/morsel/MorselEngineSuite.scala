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

package org.apache.spark.sql.execution.morsel

import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.test.SharedSparkSession

class MorselPathsSuite extends org.apache.spark.SparkFunSuite {
  test("isPosix accepts local and file URIs only") {
    assert(MorselPaths.isPosix("/tmp/store_sales.parquet"))
    assert(MorselPaths.isPosix("file:/tmp/store_sales.parquet"))
    assert(MorselPaths.isPosix("file:///tmp/store_sales.parquet"))
    assert(!MorselPaths.isPosix("hdfs://nn:8020/tmp/x"))
    assert(!MorselPaths.isPosix("s3a://bucket/x"))
    assert(!MorselPaths.isPosix(""))
  }

  test("clean strips file: prefix and keeps a leading slash") {
    assert(MorselPaths.clean("file:/tmp/x") === "/tmp/x")
    assert(MorselPaths.clean("file:///tmp/x") === "/tmp/x")
    assert(MorselPaths.clean("/tmp/x") === "/tmp/x")
  }
}

class MorselEngineSuite extends SharedSparkSession {

  import testImplicits._

  private def withMorsel[T](fn: => T): T = {
    withSQLConf("spark.sql.morsel.enabled" -> "true") {
      fn
    }
  }

  private def hasMorselHashAgg(plan: SparkPlan): Boolean = {
    plan.exists(_.isInstanceOf[MorselHashAggExec])
  }

  test("group-sum on a local file is rewritten; residual AND is not") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (1L, 2.5), (2L, 3.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)

      withMorsel {
        val ok = sql(s"SELECT k, SUM(v) FROM parquet.`$path` WHERE k > 0 GROUP BY k")
        assert(hasMorselHashAgg(ok.queryExecution.executedPlan),
          ok.queryExecution.executedPlan.toString)

        val residual = sql(
          s"SELECT k, SUM(v) FROM parquet.`$path` WHERE k > 0 AND v < 10 GROUP BY k")
        assert(!hasMorselHashAgg(residual.queryExecution.executedPlan),
          residual.queryExecution.executedPlan.toString)
        assert(residual.queryExecution.executedPlan.exists(_.isInstanceOf[HashAggregateExec]) ||
          residual.queryExecution.executedPlan.exists(_.isInstanceOf[FilterExec]))
      }
    }
  }

  test("COUNT(*) on a local file uses the footer path") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1L, 1.5), (2L, 3.0)).toDF("k", "v")
        .write.mode("overwrite").parquet(path)
      withMorsel {
        val df = sql(s"SELECT COUNT(*) FROM parquet.`$path`")
        assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[MorselCountExec]),
          df.queryExecution.executedPlan.toString)
      }
    }
  }
}
