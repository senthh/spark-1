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

"""TPC-DS product gate for Vegam.

Runs the full Spark query set (99 spec queries plus a/b splits). A query
PASSes when vegam-on row count matches vegam-off. native=yes when the
executed plan contains NativeStageExec. HDFS tables are in scope: reads
use the Java Hadoop client, not libhdfs.
"""

from __future__ import print_function

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession


def query_files(qdir):
    names = []
    for fn in sorted(os.listdir(qdir)):
        if fn.endswith(".sql"):
            names.append(fn)
    return names


def plan_text(df):
    return df._jdf.queryExecution().executedPlan().toString()


def has_native(df):
    return "NativeStageExec" in plan_text(df)


def row_count(df):
    return df.count()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queries", default="/tmp/tpcds_bench/queries")
    p.add_argument("--db", default="tpcds_sf10_parquet")
    p.add_argument("--backend", default="auto")
    args = p.parse_args()

    spark = (SparkSession.builder
             .appName("vegam-tpcds")
             .enableHiveSupport()
             .config("spark.sql.extensions", "org.apache.spark.sql.vegam.VegamExtensions")
             .config("spark.sql.vegam.enabled", "false")
             .config("spark.sql.vegam.backend", args.backend)
             .getOrCreate())
    spark.sql("USE %s" % args.db)

    files = query_files(args.queries)
    fails = []
    native_yes = 0
    t0 = time.time()
    print("queries=%s db=%s backend=%s" % (len(files), args.db, args.backend),
          flush=True)
    for fn in files:
        name = fn[:-4]
        sql = open(os.path.join(args.queries, fn)).read()
        spark.conf.set("spark.sql.vegam.enabled", "false")
        off = spark.sql(sql)
        try:
            off_n = row_count(off)
        except Exception as e:
            print("FAIL %s off-exec %s" % (name, e), flush=True)
            fails.append(name)
            continue
        spark.conf.set("spark.sql.vegam.enabled", "true")
        on = spark.sql(sql)
        native = has_native(on)
        if native:
            native_yes += 1
        try:
            on_n = row_count(on)
        except Exception as e:
            print("FAIL %s on-exec native=%s %s" % (name, native, e), flush=True)
            fails.append(name)
            continue
        ok = on_n == off_n
        print("%s %s rows=%s vs %s native=%s" % (
            "PASS" if ok else "FAIL", name, on_n, off_n,
            "yes" if native else "no"), flush=True)
        if not ok:
            fails.append(name)
    elapsed = time.time() - t0
    print("SUMMARY queries=%s pass=%s fail=%s native=%s elapsed_s=%.1f" % (
        len(files), len(files) - len(fails), len(fails), native_yes, elapsed),
          flush=True)
    print("fails=%s" % (fails,), flush=True)
    spark.stop()
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
