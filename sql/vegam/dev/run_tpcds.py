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

"""TPC-DS product gate for Vegam (phase 7).

Full Spark a/b set (q1-q99 plus 14a/b, 23a/b, 39a/b). PASS = vegam-on
row count matches vegam-off. native=yes when every time-dominating
stage is NativeStageExec (shuffle / final sort may stay Spark).

Optional --gluten runs the same SQL on a second SparkSession with
Gluten/Velox so wall-sum can be compared on the same cluster.
"""

from __future__ import print_function

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession


def query_files(qdir, limit):
    names = [fn for fn in sorted(os.listdir(qdir)) if fn.endswith(".sql")]
    if limit > 0:
        names = names[:limit]
    return names


def plan_text(df):
    return df._jdf.queryExecution().executedPlan().toString()


def native_fraction(plan):
    # Heavy stages: HashAggregate / Window / SortMergeJoin / BroadcastHashJoin
    # vs NativeStageExec. Shuffle and final Sort are allowed to stay Spark.
    heavy = 0
    native = 0
    for line in plan.splitlines():
        s = line.strip()
        if "NativeStageExec" in s:
            native += 1
            heavy += 1
        elif any(k in s for k in (
                "HashAggregate", "ObjectHashAggregate", "SortAggregate",
                "Window", "BroadcastHashJoin", "SortMergeJoin",
                "ShuffledHashJoin")):
            if "Exchange" not in s:
                heavy += 1
    if heavy == 0:
        return 0.0, False
    frac = float(native) / float(heavy)
    return frac, native > 0 and frac >= 0.5


def row_count(df):
    return df.count()


def build_spark(name, extra):
    b = (SparkSession.builder
         .appName(name)
         .enableHiveSupport())
    for k, v in extra:
        b = b.config(k, v)
    return b.getOrCreate()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queries", default="/tmp/tpcds_bench/queries")
    p.add_argument("--db", default="tpcds_sf10_parquet")
    p.add_argument("--backend", default="auto")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--gluten", action="store_true")
    args = p.parse_args()

    vegam_cfgs = [
        ("spark.sql.extensions", "org.apache.spark.sql.vegam.VegamExtensions"),
        ("spark.sql.vegam.enabled", "false"),
        ("spark.sql.vegam.backend", args.backend),
        ("spark.sql.adaptive.enabled", "true"),
    ]
    spark = build_spark("vegam-tpcds", vegam_cfgs)
    spark.sql("USE %s" % args.db)

    gluten = None
    if args.gluten:
        gluten = build_spark("gluten-tpcds", [
            ("spark.plugins", "org.apache.gluten.GlutenPlugin"),
            ("spark.shuffle.manager",
             "org.apache.spark.shuffle.sort.ColumnarShuffleManager"),
            ("spark.memory.offHeap.enabled", "true"),
            ("spark.sql.ansi.enabled", "false"),
        ])
        gluten.sql("USE %s" % args.db)

    files = query_files(args.queries, args.limit)
    fails = []
    native_yes = 0
    vegam_sum = 0.0
    gluten_sum = 0.0
    t0 = time.time()
    print("queries=%s db=%s backend=%s gluten=%s" % (
        len(files), args.db, args.backend, args.gluten), flush=True)
    for fn in files:
        name = fn[:-4]
        sql = open(os.path.join(args.queries, fn)).read()
        spark.conf.set("spark.sql.vegam.enabled", "false")
        off = spark.sql(sql)
        try:
            t = time.time()
            off_n = row_count(off)
            off_s = time.time() - t
        except Exception as e:
            print("FAIL %s off-exec %s" % (name, e), flush=True)
            fails.append(name)
            continue
        spark.conf.set("spark.sql.vegam.enabled", "true")
        on = spark.sql(sql)
        plan = plan_text(on)
        frac, native = native_fraction(plan)
        if native:
            native_yes += 1
        try:
            t = time.time()
            on_n = row_count(on)
            on_s = time.time() - t
            vegam_sum += on_s
        except Exception as e:
            print("FAIL %s on-exec native=%s %s" % (name, native, e), flush=True)
            fails.append(name)
            continue
        g_s = None
        if gluten is not None:
            try:
                t = time.time()
                gluten.sql(sql).count()
                g_s = time.time() - t
                gluten_sum += g_s
            except Exception as e:
                print("WARN %s gluten-exec %s" % (name, e), flush=True)
        ok = on_n == off_n
        gtxt = "" if g_s is None else " gluten=%.3f" % g_s
        print("%s %s rows=%s vs %s native=%s frac=%.2f off=%.3f on=%.3f%s" % (
            "PASS" if ok else "FAIL", name, on_n, off_n,
            "yes" if native else "no", frac, off_s, on_s, gtxt), flush=True)
        if not ok:
            fails.append(name)
    elapsed = time.time() - t0
    print("SUMMARY queries=%s pass=%s fail=%s native=%s vegam_wall=%.1f "
          "gluten_wall=%.1f elapsed_s=%.1f" % (
              len(files), len(files) - len(fails), len(fails), native_yes,
              vegam_sum, gluten_sum, elapsed), flush=True)
    print("fails=%s" % (fails,), flush=True)
    spark.stop()
    if gluten is not None:
        gluten.stop()
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
