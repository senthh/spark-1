# Native pipeline design: beat Gluten+Velox, compete with Photon

Status: proposal
Audience: vegam engine on `vegam`
Target: Databricks Photon and Meta Velox (via Gluten), not Spark whole-stage codegen
Non-goal: more hand-written `unordered_map` operators

## 1. What we are competing with

Spark JVM codegen is the wrong opponent. On a single-file `filter + group by + sum`
it is already a tight loop. Our typed warm run was 0.21-0.29s vs Spark 0.23-0.32s.
That is not a Photon-class result.

The real opponents:

- **Gluten + Velox**: same Velox kernels we would want, plus a Substrait adapter,
  Spark partition = task, and frequent fallback to JVM. Smile SF10 Q1-Q15
  (checkpoint only): Velox ~36s, Spark ~64-77s. The product gate is **all
  TPC-DS queries** (99 official; Spark's tree is ~103 SQL files with a/b
  splits: 14a/14b, 23a/23b, 39a/39b).
- **Photon**: native operators inside the Spark executor, no Substrait, fused
  stages, hash *and* sort/merge, years of kernel work. That is the bar.

If we cannot state why we beat Gluten on the same Velox kernels, the design is
wrong. If we cannot name the Photon features we still lack, we will over-claim.

## 2. Steal the best of three, not the brand names

### Morsel (HyPer SIGMOD 2014) -- keep this as *our* engine

The paper's unit is ~10K rows and a work-stealing scheduler, not "C++".

- **Intra-split parallelism.** Spark and Gluten launch one task per partition /
  file split. A 2-row-group file uses 2 cores. Morsels cut a split into 10K-row
  units and steal across cores. That is the one thing Gluten does not inherit
  from Spark's scheduler.
- **Pipeline fusion.** Scan, residual filter, and agg share one worker loop.
  No filtered `RecordBatch` allocation, no scan-then-reset-then-agg barrier.
- **NUMA / locality.** Workers pin and steal locally. Photon cares about this;
  Gluten usually does not.

What we must *not* keep from the current prototype: Arrow `ReadRowGroup` of a
whole group, `std::unordered_map` per row, POSIX-only paths, Substrait-free
rewrite that only matches one group + one sum.

### Velox -- take the kernels, not the engine wrapper

Velox already won the inner loops we measured as the gap:

- **TableScan**: page-level predicates, dictionary filter, lazy materialization.
- **HashAggregation / HashProbe**: vectorized, open-addressed, SIMD-friendly.
- **Expr eval**: one compile of the residual expression, batch execution.

Gluten's cost is not these kernels. It is *getting to them*: Spark plan ->
Substrait -> Velox plan, Vector <-> ColumnarBatch at every Spark operator
edge, and fallback when a type or expr is missing.

We call Velox operators as a library from a morsel pipeline. We do not become
another Gluten.

### Trino -- take the *policy*, especially sort

Trino's Pages/drivers look like morsels. The unique piece is **when not to hash**:

- **Sort-agg** when the input is clustered on the group key (common on date_sk
  fact layouts). Streaming agg, tiny memory, no hash table.
- **Sort-merge join** when both sides are ordered or the build side is huge.
  Hash join is not free; Photon picks merge more often than Gluten.
- **Spillable sort** for window, top-K, and order-by. Production-grade spill
  is how Trino survives larger-than-memory sorts.

We do not port Trino's Java. We port the decision: hash vs sort-agg vs
merge-join, using Velox or a ClickHouse radix sort we already sketched.

## 3. Architecture

```
  Spark Catalyst + AQE + Hadoop FS          stay
           |
  NativeStage rewrite (no Substrait)        ours, Photon-shaped
           |
  MorselScheduler (10K, steal, NUMA)        HyPer
           |
  Fused pipeline (one C++ stage)            ours
      Velox TableScan / remaining filter
      Velox HashAgg or Trino-policy sort-agg
      Velox HashJoin or merge-join
      Velox / radix sort for window, top-K
           |
  Convert once: vectors -> Spark shuffle    only at stage edges
```

Rules:

1. Spark owns SQL, analysis, AQE, files, and shuffle *service*.
2. One rewrite produces a `NativeStage` leaf/pipeline, not a Substrait blob.
3. Inside the stage, data stays Velox vectors. No JNI per operator.
4. JNI once per task: start pipeline, pull output pages.
5. If the rewrite cannot cover the whole stage, leave it on Spark. Never a
   mixed fused plan (that is Gluten's slow path).
6. Storage: Hadoop `FSDataInputStream` (or Spark file source) feeds a Velox
   in-memory / buffer reader. No libhdfs `dlopen`.

## 4. Why this beats Gluten+Velox

Same kernels, less tax, better scheduling, better join/agg *shape*.

### 4.1 Kill the adapter (Photon-shaped)

Gluten translates every expression twice and fails open into JVM mid-query.
A half-native plan loses fusion and pays convert on both sides.

We only rewrite when the *entire Spark stage* is representable. That is how
Photon stays fast: native or not, not "native until this Cast".

Expected win: 10-25% on queries Gluten already runs fully native, from fewer
copies and no Substrait. Larger win on queries Gluten partially fallbacks.

### 4.2 Morsels inside a Spark split (HyPer)

Gluten's parallelism is Spark's. TPC-DS SF10 `store_sales` is many files, but
dims and some queries are few splits. Our 2-RG local file showed the issue:
2 tokens, 8 cores idle unless we over-split.

Velox *can* split internally; Gluten's task model often does not steal across
cores on one split the way HyPer does.

Expected win: high on few-split / skew queries (dims, inventory, some
store-only plans), small on already well-partitioned fact scans.

### 4.3 Convert once per stage, not per operator

Gluten: Velox op -> ColumnarBatch -> Spark -> next Velox op.
Photon / this design: one native pipeline, one convert at shuffle/collect.

On a scan-filter-agg-join stage (the snowflake class, a large slice of the
99), this is the copy tax that makes "we use Velox" still slower than Photon
using similar ideas. Window and rollup stages have the same tax if Gluten
fallback-splits them.

### 4.4 Trino policy: stop hashing everything

Gluten+Velox hash-joins and hash-aggs by default. TPC-DS facts are often
physically ordered by date. Sort-agg and merge-join:

- less memory than a full hash table on a large build;
- better sequential scan + merge;
- Photon-class behavior Gluten rarely selects.

Expected win: memory-bound joins and high-cardinality agg (year-over-year
pairs, catalog/web/store unions, inventory rollups), not the 1639-group
local demo.

### 4.5 What we will not claim

- Day-one faster than Photon on all 99 TPC-DS queries. Photon kernels are older.
- Faster than Velox *kernels*. We are using them.
- A 3-4x over Spark JVM on single-file group-sum. That query is the wrong
  contest.

The claim is: **Gluten+Velox is Velox kernels plus an adapter. We are Velox
kernels plus a HyPer scheduler plus Photon-style stage fusion plus Trino
hash/sort choice.** That combination is what can beat Gluten. Closing on
Photon is kernel quality and coverage over time, not a new translator.

## 5. Full TPC-DS, not Q1-Q15

Q1-Q15 is a smoke slice. It misses most of what Gluten and Photon actually
spend time on: windows, grouping sets, multi-fact unions, exists/in, inventory
self-joins, and outer joins. A design that only "covers Q1-Q15 natively" will
look good on a short scoreboard and lose the official 99.

Spark's `tpcds` query set is the 99 spec queries plus a/b files (14, 23, 39).
Every gate below uses **that full set**. Q1-Q15 times we already have are a
checkpoint, not the product metric.

### 5.1 Query classes (all 99)

| Class | Examples | Native must have |
|---|---|---|
| Scan + filter + simple agg | q3, q7, q12, q19, q42, q43, q52, q55, q73, q98 | Velox scan + hash/sort-agg |
| Snowflake fact-dim join + agg | q6, q15, q19, q26, q40, q42, q62, q68, q71, q96 | multi-join pipeline, broadcast/shuffle |
| Year-over-year / pair | q4, q11, q74 | two-scan self-compare, or join + window |
| Multi-fact union | q5, q14a/b, q80 | union of store/catalog/web, same schema fuse |
| Window / rank / lag | q2, q20, q44, q47, q53, q57, q63, q67, q89 | Trino-quality sort + Velox window |
| Exists / IN / scalar subq | q1, q6, q10, q16, q30, q32, q35, q41, q45, q69, q81, q92 | rewrite to join/semi-join, not JVM subquery |
| Outer join | q10, q35, q81 | Velox left/right/full, null-aware |
| Inventory / rolling | q21, q22, q23a/b, q39a/b | self-join + window or grouped ratios |
| Heavy join / explode | q64, q72, q85, q94, q95 | hash + merge policy, spill |
| Reporting / grouping sets | q9, q27, q36, q67, q70, q86 | rollup/cube or expand + fuse |
| Intersect / except / distinct | q8, q14, q38, q87 | native set ops or rewrite to join |
| Order-by / top-K / limit | many + q28, q88 | spillable sort, not collect-on-driver |

If a class has no kernel, those queries stay 100% Spark (fail closed). Gluten
often *partially* natives them and pays convert. We would rather be fully
Spark on q67 than half-native and wrong/slow.

### 5.2 Why the adapter tax grows after Q15

Later queries have more expressions Gluten fails to map (decimal rounding,
`LIKE`, `ROLLUP`, correlated exists, char padding). Each fallback splits a
stage. Full-suite Gluten time is not "Q1-Q15 * 6"; it is Q1-Q15 plus a long
tail of mixed plans. That tail is where Photon-style fail-closed + Trino
sort/window should beat Gluten, if we implement the class, or stay on Spark
without pretending we did.

### 5.3 Coverage rule

A query counts as **native** only if every *time-dominating* stage is a
NativeStage (scan+join+agg+window of the facts). Shuffle and final sort may
stay Spark. A query with one native filter and a JVM window does **not**
count as native. Report three numbers on the full set:

- PASS / FAIL / row-count match (all 99)
- wall-sum vs Gluten on the same cluster and SF
- fraction of queries whose heavy stages are NativeStage

## 6. Operator dispatch

| Shape | Engine | Why | TPC-DS classes |
|---|---|---|---|
| Parquet/ORC scan + residual expr | Velox TableScan | page skip, dictionary | all |
| 1..N group, sum/count/min/max | Velox HashAgg, or sort-agg if clustered | Velox table; Trino when ordered | simple agg, snowflake |
| Equijoin, small/medium build | Velox HashJoin | proven | snowflake, pair |
| Equijoin, large/sorted both sides | merge-join | Trino/Photon | inventory, heavy join |
| Semi/anti/outer join | Velox | exists/IN/outer | q1, q10, q16, q35, q81 |
| Filter/project in a native stage | Velox expr | do not allocate a new batch | all |
| Union same schema | fuse scans + concat | avoid N Spark unions | q5, q14, q80 |
| Window, rank, lag/lead | sort + Velox window | Trino sort is the product | q2, q47, q57, q67 |
| Grouping sets / rollup / cube | Velox or expand+fuse | do not fall to JVM ObjectHash | q9, q36, q67, q70 |
| Order-by, top-K, limit | radix (int) or spill sort | | reporting, q28, q88 |
| Shuffle, AQE, broadcast | Spark | do not rewrite the cluster | all |
| Uncovered expr/type/UDF | whole stage stays Spark | fail closed | remainder |

## 7. Current prototype vs this design

| | Today | This design |
|---|---|---|
| Opponent | Spark JVM | Gluten, Photon |
| Scan | Arrow `ReadRowGroup` | Velox TableScan + Hadoop bytes |
| Agg | `unordered_map` per row | Velox HashAgg / sort-agg |
| Parallelism | 1 token / RG or file | 10K morsels, steal |
| Files | POSIX only | any Spark FS |
| Plan | 1 group + 1 sum | whole native stage, all TPC-DS classes |
| Join | none | Velox hash, then merge, semi/anti/outer |
| Window / union / rollup | none (skip) | required for full 99 |
| Adapter | none (good) | still none (keep) |

Keep from today: fail-closed filters, POSIX/HDFS gate until the Hadoop byte
source exists, Partial + Spark Final for decimal unscaled, reject-reason log.

## 8. Phases (order is the design)

Kernel check stays first. After that, work is **by TPC-DS class**, not
"finish Q1-Q15 then stop".

1. **Velox scan + hashagg behind the existing rewrite.** Same SQL as now
   (typed multi-file group-sum). Measure vs Gluten on *that* query, not vs
   Spark. Success: warm time at or under Gluten, `kt/vt` is Velox vectors,
   no `unordered_map` on the hot path.
2. **Morsel the Velox split.** 10K-row output batches from TableScan, steal
   across threads inside one Spark task. Success: 2-RG file uses all
   `local[8]` cores.
3. **NativeStage rewrite** for Filter+Project+Agg+one join, fail closed.
   Hadoop byte source. Success: a 2-table date join + group-sum matches Spark
   row-by-row and is measured against Gluten. Unlocks snowflake class
   (q6, q15, q19, ...).
4. **Trino policy.** Clustered sort-agg and merge-join when file order or
   stats say so. Success: year-over-year and union queries (q4, q11, q14a/b)
   time or memory vs Gluten hash-only.
5. **Semi/anti/outer + subquery rewrite.** Exists/IN become joins. Success:
   q1, q10, q16, q35, q81 PASS native on the heavy stages.
6. **Window + grouping sets + union fuse.** Sort/window and rollup. Success:
   q2, q47, q57, q67, q70, q5/q80 no longer skip for "window/rollup".
7. **Full suite.** Expr allow-list until **all 99** PASS (row counts match
   Spark). Report wall-sum vs Gluten on the same SF10 (then SF100). A query
   that cannot be fully staged stays Spark; it still must PASS. Do not call
   the suite "native" until the class table above is implemented.

Do not start joins until phase 1 beats or ties Gluten on the group-sum we
already have. That is the only honest kernel check.

Do not treat a green Q1-Q15 scoreboard as phase 7. Q16-Q99 include the
windows, rollups, and explode joins that decide Gluten vs Photon.

## 9. Risks

- **Velox build.** Folly, SIMD, Arrow ABI. Treat Velox as a pinned submodule
  on the 160 builder, ship `.so` + rpath like today. Do not install cluster
  RPMs.
- **Memory.** One Velox memory pool per executor, Spark off-heap reserved.
  Two pools without a budget is how Gluten OOMs.
- **Correctness.** Decimal Partial unscaled, null groups, `>=` vs `>`. Keep
  the fail-closed rule. Validate keys and sums, not row counts.
- **HDFS.** Byte-range reads through Hadoop, not libhdfs.

## 10. Success metrics (only these)

| Gate | Metric | Opponent |
|---|---|---|
| Kernel | typed local group-sum, warm median | Gluten+Velox same file |
| Scheduler | same file, 2 RGs, CPU all cores | Gluten one-split task |
| Stage | `store_sales ⋈ date_dim` + group-sum, row-by-row | Gluten |
| Classes | each class in 5.1 has >=1 representative PASS native | Gluten same query |
| Checkpoint | TPC-DS SF10 Q1-Q15 sum, all PASS | Gluten ~36s (known) |
| **Suite (the gate)** | **TPC-DS SF10 all 99 (Spark a/b files), all PASS, row counts match** | **Gluten full-suite wall-sum (measure; we do not have it yet), then Photon if available** |
| Scale | same full set at SF100 when SF10 suite wins | Gluten / Photon |

Spark JVM times are a regression check, not the goal.

Q1-Q15 ~36s is the only Gluten number we have today. It is a checkpoint.
Shipping on that number is optimizing the easy third of TPC-DS.
