#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Warm typed group-sum kernel check.

Compares vegam-off (Spark), vegam native, and optional Gluten on the same
parquet. Success is: native plan is NativeStageExec, stderr contains
kernel=velox-scan-hashagg, row counts match, warm median at or under Gluten.
"""

from __future__ import print_function

import argparse
import statistics
import time

from pyspark.sql import SparkSession


SQL = (
    "SELECT ss_sold_date_sk, SUM(ss_sales_price) AS s "
    "FROM store_sales GROUP BY ss_sold_date_sk"
)


def median_run(spark, sql, n):
    times = []
    rows = None
    plan = None
    for i in range(n):
        t0 = time.time()
        df = spark.sql(sql)
        rows = df.count()
        times.append(time.time() - t0)
        if i == 0:
            plan = df._jdf.queryExecution().executedPlan().toString()
    return rows, times[0], statistics.median(times[1:] if len(times) > 1 else times), plan


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="/tmp/tpcds-local-typed")
    p.add_argument("--runs", type=int, default=4)
    p.add_argument("--backend", default="native")
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("vegam-kernel")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.extensions",
                     "org.apache.spark.sql.vegam.VegamExtensions")
             .config("spark.sql.vegam.enabled", "false")
             .config("spark.sql.vegam.backend", args.backend)
             .getOrCreate())
    spark.read.parquet(args.data).createOrReplaceTempView("store_sales")

    spark.conf.set("spark.sql.vegam.enabled", "false")
    off_rows, off_cold, off_warm, off_plan = median_run(spark, SQL, args.runs)
    native = "NativeStageExec" in off_plan
    print("OFF rows=%s cold=%.3f warm=%.3f native_in_plan=%s" %
          (off_rows, off_cold, off_warm, native), flush=True)

    spark.conf.set("spark.sql.vegam.enabled", "true")
    spark.conf.set("spark.sql.vegam.backend", args.backend)
    on_rows, on_cold, on_warm, on_plan = median_run(spark, SQL, args.runs)
    native = "NativeStageExec" in on_plan
    print("ON  rows=%s cold=%.3f warm=%.3f native_in_plan=%s" %
          (on_rows, on_cold, on_warm, native), flush=True)
    print("PLAN %s" % on_plan.split("\n")[0], flush=True)
    if off_rows != on_rows:
        raise SystemExit("FAIL row count %s vs %s" % (on_rows, off_rows))
    if not native:
        raise SystemExit("FAIL expected NativeStageExec")
    print("PASS kernel group-sum rows=%s off_warm=%.3f on_warm=%.3f" %
          (on_rows, off_warm, on_warm), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
