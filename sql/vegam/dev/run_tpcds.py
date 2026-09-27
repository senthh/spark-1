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

"""TPC-DS a/b gate for a native engine (Vegam or Gluten).

Each query runs with the engine off (the control) and on, in the same
session, toggled through the engine's session conf. Both sides are timed on
collect() of the real query; count() is not used because it lets the
optimizer prune columns and aggregates. With --runs N the sides alternate
N times and the fastest run of each side is reported.

PASS = the two results are equal by value:
  - rows are compared as sorted multisets (ORDER BY ties may reorder),
  - decimal, integer, string, date values must match exactly,
  - float / double values must match within --rel-tol.

native=yes when at least half of the time-dominating operators run in the
engine (shuffle / final sort may stay Spark). Results go to stdout and the
--out TSV.
"""

from __future__ import print_function

import argparse
import datetime
import decimal
import math
import os
import sys
import time

from pyspark.sql import SparkSession


HEAVY_OPS = (
    "HashAggregate", "ObjectHashAggregate", "SortAggregate", "Window",
    "BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin")

# engine -> (session toggle, plan-line marker of a native operator)
ENGINES = {
    "vegam": ("spark.sql.vegam.enabled", "NativeStageExec"),
    "gluten": ("spark.gluten.enabled", "Transformer"),
}


def query_files(qdir, only, limit):
    # Reject AppleDouble companions (._q1.sql) macOS leaves behind.
    names = [fn for fn in sorted(os.listdir(qdir))
             if fn.endswith(".sql") and not fn.startswith("._")]
    if only:
        want = set(q.strip() for q in only.split(",") if q.strip())
        names = [fn for fn in names if fn[:-4] in want]
    if limit > 0:
        names = names[:limit]
    return names


def plan_text(df):
    return df._jdf.queryExecution().executedPlan().toString()


def native_fraction(plan, marker):
    heavy = 0
    native = 0
    for line in plan.splitlines():
        s = line.strip()
        if marker in s and "Exchange" not in s:
            native += 1
            heavy += 1
        elif any(k in s for k in HEAVY_OPS) and "Exchange" not in s:
            heavy += 1
    if heavy == 0:
        return 0.0, False
    frac = float(native) / float(heavy)
    return frac, native > 0 and frac >= 0.5


def norm_value(v):
    # Sort key and comparison form. Floats keep their value; None sorts first.
    if v is None:
        return (0, "")
    if isinstance(v, bool):
        return (1, v)
    if isinstance(v, (int, decimal.Decimal)):
        return (2, decimal.Decimal(v))
    if isinstance(v, float):
        return (3, v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return (4, v.isoformat())
    return (5, str(v))


def sort_key(row):
    out = []
    for v in row:
        tag, x = norm_value(v)
        if tag == 3:
            # Round so tiny float drift does not change the sort order.
            x = 0.0 if x == 0 or math.isnan(x) else float("%.9g" % x)
        out.append((tag, x))
    return out


def values_equal(a, b, rel_tol):
    ta, xa = norm_value(a)
    tb, xb = norm_value(b)
    if ta == 3 or tb == 3:
        if ta == 0 or tb == 0:
            return ta == tb
        fa, fb = float(xa), float(xb)
        if math.isnan(fa) or math.isnan(fb):
            return math.isnan(fa) and math.isnan(fb)
        return math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=1e-9)
    return ta == tb and xa == xb


def compare(off_rows, on_rows, rel_tol):
    """Returns None when equal, else a short description of the first diff."""
    if len(off_rows) != len(on_rows):
        return "rows %d vs %d" % (len(on_rows), len(off_rows))
    a = sorted((tuple(r) for r in off_rows), key=sort_key)
    b = sorted((tuple(r) for r in on_rows), key=sort_key)
    for i, (ra, rb) in enumerate(zip(a, b)):
        if len(ra) != len(rb):
            return "row %d width %d vs %d" % (i, len(rb), len(ra))
        for c, (va, vb) in enumerate(zip(ra, rb)):
            if not values_equal(va, vb, rel_tol):
                return "row %d col %d: on=%r off=%r" % (i, c, vb, va)
    return None


def timed_collect(spark, sql):
    """Returns (rows, seconds, executed plan text after AQE finished)."""
    t = time.time()
    df = spark.sql(sql)
    rows = df.collect()
    return rows, time.time() - t, plan_text(df)


def build_spark(name, extra):
    b = SparkSession.builder.appName(name).enableHiveSupport()
    for k, v in extra:
        b = b.config(k, v)
    return b.getOrCreate()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queries", default="/tmp/tpcds_bench/queries")
    p.add_argument("--db", default="tpcds_sf10_parquet")
    p.add_argument("--engine", default="vegam", choices=sorted(ENGINES))
    p.add_argument("--backend", default="auto", help="Vegam backend")
    p.add_argument("--only", default="", help="comma list, e.g. q3,q7")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--runs", type=int, default=1)
    p.add_argument("--stat", default="fastest", choices=("fastest", "mean"),
                   help="fastest run wins, or the mean of runs")
    p.add_argument("--rel-tol", type=float, default=1e-9)
    p.add_argument("--out", default="tpcds_ab.tsv")
    args = p.parse_args()

    toggle, marker = ENGINES[args.engine]
    confs = [(toggle, "false"), ("spark.sql.adaptive.enabled", "true")]
    if args.engine == "vegam":
        confs += [
            ("spark.sql.extensions", "org.apache.spark.sql.vegam.VegamExtensions"),
            ("spark.sql.vegam.backend", args.backend),
        ]
    spark = build_spark("%s-tpcds" % args.engine, confs)
    spark.sql("USE %s" % args.db)

    files = query_files(args.queries, args.only, args.limit)
    fails = []
    native_yes = 0
    off_sum = 0.0
    on_sum = 0.0
    t0 = time.time()
    out = open(args.out, "w")
    out.write("query\tstatus\trows\tnative\tfrac\toff_s\ton_s\tdetail\n")
    print("queries=%s db=%s engine=%s runs=%d" % (
        len(files), args.db, args.engine, args.runs), flush=True)
    for fn in files:
        name = fn[:-4]
        sql = open(os.path.join(args.queries, fn), encoding="utf-8",
                   errors="replace").read()
        off_s = on_s = float("inf")
        off_times = []
        on_times = []
        off_rows = on_rows = None
        frac, native = 0.0, False
        error = None
        for _ in range(max(1, args.runs)):
            spark.conf.set(toggle, "false")
            try:
                off_rows, t, _ = timed_collect(spark, sql)
                off_times.append(t)
                off_s = min(off_s, t)
            except Exception as e:
                error = ("OFF_ERROR", e)
                break
            spark.conf.set(toggle, "true")
            try:
                on_rows, t, plan = timed_collect(spark, sql)
                on_times.append(t)
                on_s = min(on_s, t)
                frac, native = native_fraction(plan, marker)
            except Exception as e:
                error = ("ON_ERROR", e)
                break
        if error is not None:
            status, e = error
            print("%s %s native=%s %s" % (status, name, native, e), flush=True)
            out.write("%s\t%s\t\t%s\t%.2f\t%s\t\t%s\n" % (
                name, status, native, frac,
                "" if off_s == float("inf") else "%.3f" % off_s,
                str(e).replace("\n", " ")[:200]))
            fails.append(name)
            continue
        if args.stat == "mean":
            off_s = sum(off_times) / len(off_times)
            on_s = sum(on_times) / len(on_times)
        off_sum += off_s
        on_sum += on_s
        if native:
            native_yes += 1
        diff = compare(off_rows, on_rows, args.rel_tol)
        status = "PASS" if diff is None else "FAIL"
        print("%s %s rows=%d native=%s frac=%.2f off=%.3f on=%.3f%s" % (
            status, name, len(off_rows), "yes" if native else "no", frac,
            off_s, on_s, "" if diff is None else " diff=" + diff), flush=True)
        out.write("%s\t%s\t%d\t%s\t%.2f\t%.3f\t%.3f\t%s\n" % (
            name, status, len(off_rows), "yes" if native else "no", frac,
            off_s, on_s, diff or ""))
        out.flush()
        if diff is not None:
            fails.append(name)
    out.close()
    print("SUMMARY queries=%d pass=%d fail=%d native=%d off_wall=%.1f "
          "on_wall=%.1f elapsed_s=%.1f" % (
              len(files), len(files) - len(fails), len(fails), native_yes,
              off_sum, on_sum, time.time() - t0), flush=True)
    print("fails=%s" % (fails,), flush=True)
    spark.stop()
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
