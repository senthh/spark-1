# Design Doc — Partial-Aggregate Push-down for Spark 5.0 (Gluten fork)

Status: Draft v1 (for review before implementation)
Base commit: `f4ebb0d4e79` (master, branch `ms_paper`), Spark `5.0.0-SNAPSHOT`
Reference: "New Query Optimization Techniques in the Spark Engine of Azure Synapse" (Modi et al., PVLDB 2022, §4)
Goal: Add a first-class **logical partial aggregate** operator that the Catalyst optimizer can
seed and push down through joins / unions / projects / selects / expands, so that rows are
collapsed *before* expensive exchange/join/union boundaries. This shrinks the data that Velox
and the shuffle touch — directly on the Gluten path.

---

## 1. Mental model (recap)

A **partial aggregate** `γ(keys, [aggs])` is an *optional, always-safe* pre-aggregation. Dropping
it never changes results; keeping it collapses rows cheaply (one hash-table build) before costly
operators. It is fundamentally different from full `GROUP BY` push-down: it never removes the real
final aggregate, so correctness is unconditional (no FKs/primary-key pre-conditions).

The only cost is the extra hash-map per partial agg. So every `γ` must be **costed independently**
and kept only if it reduces exchanged rows past a threshold (`rr = rows_after / rows_before < Th`).

Spark 5 **already** does the fixed physical `Partial` → shuffle → `Final` split for a group-by's own
input (`AggUtils.planAggregateWithoutDistinct`). This feature adds the *logical* operator and the
*rules* that let partial aggs be placed **mid-tree** (below joins/unions) that the fixed split cannot
express.

---

## 2. What we create vs. what we change — summary table

| Artifact | Action | Package |
|---|---|---|
| `PartialAggregate` (new logical op) | **CREATE** | `catalyst.plans.logical` |
| `AggregateMode.Partial*` reuse (no new modes) | **REUSE** | `catalyst.expressions` |
| `SeedPartialAggregate` rule | **CREATE** | `catalyst.optimizer` |
| `PushPartialAggregateThroughJoin` rule | **CREATE** | `catalyst.optimizer` |
| `PushPartialAggregateThroughUnion` rule | **CREATE** | `catalyst.optimizer` |
| `PushPartialAggregateThroughUnary` rule (Project/Filter/Expand/Window) | **CREATE** | `catalyst.optimizer` |
| `PartialAggregateCost` (cost gate) | **CREATE** | `catalyst.optimizer` (or `plans.logical.statsEstimation`) |
| `SeedPartialAggregateFromSemiJoin` / `FromIntersect` rules | **CREATE** | `catalyst.optimizer` |
| `Optimizer` rule registration | **CHANGE** | `catalyst.optimizer.Optimizer` |
| `BasicStatsPlanVisitor` (handle `PartialAggregate`) | **CHANGE** | `catalyst.plans.logical.statsEstimation` |
| `BasicOperators` physical strategy (lower `PartialAggregate`) | **CHANGE** | `sql.execution` (SparkStrategies) |
| `EliminatePartialAggregate` (optional cleanup, default off) | **CREATE (optional)** | `catalyst.optimizer` |
| SQL conf flags | **CHANGE** | `catalyst.SQLConf` |
| Tests (suite per rule + `SQLQueryTestSuite` TPCDS ao$) | **CREATE** | catalyst/sql test trees |

No new physical exec operator is required: the pushed-down `γ` lowers to the existing physical
partial `HashAggregateExec` (mode = `Partial`), which Velox already executes natively.

---

## 3. Component detail

### 3.1 `PartialAggregate` — new logical operator (CREATE)

Analogous to `Aggregate` in `basicLogicalOperators.scala:1269`:

```scala
case class PartialAggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[AggregateExpression],   // mode = Partial
    child: LogicalPlan)
  extends UnaryNode { ... }
```

- Patterns tag: reuse `AGGREGATE` tree pattern so existing pruning/visitors see it; add a
  `PARTIAL_AGGREGATE` pattern if fine-grained control is needed.
- `computeStats`: falls through to the new `BasicStatsPlanVisitor` handler (§3.7) so row-count
  estimates after the partial agg are available to the cost gate.
- Since the paper rewrites `count(*) → sum(1)` first (partial=sum(1), final=count), the
  `aggregateExpressions` carried here are in **Partial mode** with matching partial functions.
- **Validity invariant** (enforced by seed rule, asserted by `checkinput`-style validation):
  a `PartialAggregate` must not be the child of another `PartialAggregate` from a different seed —
  keep exactly one partial agg per stage (paper §4.3 heuristic #1).

### 3.2 Operator semantics — no new AggregateMode

Reuse existing `AggregateMode.{ Partial, PartialMerge, Final, Complete }`. The partial agg below a
join/union is `Partial`; the merge above it is `Final` (created by lowering, not by a new mode).
This guarantees Velox compatibility — it already sees the standard partial/final pipe.

### 3.3 `SeedPartialAggregate` rule (CREATE)

`Rule[LogicalPlan]`. Derives a `γ` from three sources (paper §4.1):

1. **From group-by** (`Aggregate`): insert `PartialAggregate(groupingExprs, partialAggExprs, child)`
   *above* the aggregate's child — i.e. `Aggregate(g, [f])` becomes
   `Aggregate(g, [f])` with a `γ` pushed onto the child. (Note: Spark's physical planning already
   adds Partial before the shuffle *for the final group-key*; this seeding is at the *logical* level
   so the γ can then be pushed further up and down.)
2. **From left semi-join** (§4.1 / Fig 8b): `LeftSemi(a1=a2)(L, R)` → insert a *distinct* partial agg
   on the **right** child keyed on the equi-join keys: `γ(a2, [])`. (Mirror for right semi-join.)
3. **From intersect** (§4.1 / Fig 8c): insert a distinct `γ` on **each** input, keyed on that input's
   columns (intersect output is a set → dedupe inputs is safe).

Each seeded γ is tagged with its source seed id so the costing pass can treat the fan-out of a single
seed as one candidate group (paper: "only a single partial-aggregate per stage" + "top-most before
the exchange").

### 3.4 Push-down rules (CREATE)

These move an existing `γ` down the tree, operator by operator. All rules are
`Rule[LogicalPlan]` transforms keyed on finding a `PartialAggregate` whose child matches.

- **`PushPartialAggregateThroughJoin`** (§4.2 / Fig 9):
  Pre-condition: each agg argument must come from exactly one join input (split by child's
  `outputSet`). Then split keys and aggs between left/right; keys on each side = (shared parent keys ∩
  that side's available cols) ∪ (that side's join keys). Emit `γ` on left and `γ` on right, keep a
  `Project` above the join that re-combines partial columns, and keep the parent γ optional.
- **`PushPartialAggregateThroughUnion`** (§4.2 / Fig 10): no pre-conditions. Push a copy of the γ onto
  each union input (agg functions → their partial computations, `count→sum(1)`). `sum`/`min`/`max`/
  `count` supported.
- **`PushPartialAggregateThroughUnary`**: through `Project` and `Filter` (extend keys with predicate
  columns; only push through Project if the project expr is not involved in agg arguments — paper
  §4.2) and through `Expand` (used for `ROLLUP/CUBE` and `count(distinct)`), and `Window` (leaf —
  Γ final sits above). This makes the final-aggregate always sit correctly below the seed's ecosystem.
- **Ordering constraint** (§6 of this doc): push-down must run *after* the "infer filters / project
  pushdown" batches so that the tree shape the γ moves through is already simplified; run *before*
  the distinct-elimination finalization so the physical planner sees a clean Partial/Final shape.

### 3.5 `SeedPartialAggregateFromSemiJoin` / `SeedPartialAggregateFromIntersect` (CREATE)

Separate small rules (or one rule, three match arms — implement as one file with three cases is
cleaner). Guarded so they never fire without the corresponding SQLConf flag.

### 3.6 `PartialAggregateCost` — the cost gate (CREATE)

Implements the paper §4.3 two heuristics:

- **Per-stage single γ**: among the γ's pushed from one seed, keep only the top-most (right before an
  exchange).
- **Threshold**: keep a γ iff `rr = rowsAfter / rowsBefore < Th` (default `Th = 0.5`, configurable).
  `rowsAfter/Before` computed via the statistics on the plan above/below the γ.

Also implement the paper's **combinatorial-blow-up mitigation** (§4.3): when a γ key overlaps the
stage's partitioning key, scale that key's NDV down by `degreeOfParallelism` (`spark.sql.shuffle.partitions`
or the AQE partition count). And for broadcast-join stages, evaluate reduction ratio along the chain
from the large input.

This is the closest thing to "model" in the design. It is a lightweight independent cost, **not** the
AQE `costing.scala` — deliberately isolated so it doesn't perturb the existing physical cost model.

### 3.7 Lowering to physical — `BasicOperators` (CHANGE)

In the aggregate physical planning path (`SparkStrategies.scala:735+`), add a match arm:
`case p: PartialAggregate => planPartialAggregate(p)` which calls
`AggUtils.createAggregate(requiredDist = None, mode=Partial, grouping=..., aggs=..., resultExprs, child)`.
The output becomes the standard begin-of-pipe partial hash-aggregate. Because this reuses the same
exec paths, **Gluten/Velox executes it unchanged** — the only "new" surface is the logical op the
optimizer moved around.

### 3.8 `BasicStatsPlanVisitor` (CHANGE)

Add a `visitPartialAggregate` handler so `PartialAggregate` computes `rowCount` = estimated distinct
groups (product of NDVs of `groupingExpressions`, with the partitioning-key `/dop` scaling from §3.6).
This feeds both the cost gate and normal AQE stats. Without it, stats propagation below the γ is
broken (it'd estimate unbounded), which the paper flags as the combinatorial blow-up.

### 3.9 `EliminatePartialAggregate` (CREATE, optional, default **off**)

A safety/fallback rule that removes any remaining `PartialAggregate` from the logical plan (walking
it back to `Aggregate` semantics) — used as an escape hatch if a query hits a path where a γ can't be
lowered (e.g. unsupported agg function), so we never throw on correctness. Guards the "always
optional" invariant in practice.

### 3.10 Conf flags (CHANGE — `SQLConf`)

```scala
spark.sql.optimizer.partialAggregatePushdown.enabled        (default true)
spark.sql.optimizer.partialAggregatePushdown.threshold       (default 0.5)
spark.sql.optimizer.partialAggregatePushdown.eagerMaxDepth   (optional cap on push-down depth)
```
All rules gated on the `enabled` flag; cost gate uses `threshold`.

---

## 4. Rule pipeline placement (CHANGE — `Optimizer.scala`)

Current relevant structure (`Optimizer.scala`):
- `operatorOptimizationBatch` (fixedPoint) already contains `RemoveRedundantAggregates`,
  `PushDownLeftSemiAntiJoin`, `CombineUnions`.

Proposed insertion — **new batch `Partial Aggregate Optimizations`** between the standard
operator-optimization batches and the "infer filters" re-run:

```scala
Batch("Partial Aggregate Optimizations", fixedPoint,
  SeedPartialAggregate,                       // derives γ from Aggregate/semi-join/intersect
  PushPartialAggregateThroughJoin,
  PushPartialAggregateThroughUnion,
  PushPartialAggregateThroughUnary,
  PartialAggregateCost)                       // prunes non-beneficial γ's
```

Rationale:
- Runs after `RemoveRedundantAggregates`/`CombineUnions` so it doesn't re-expand already-deduped trees;
- Runs before the final physical-partial split so the planner sees one coherent γ placement;
- `fixedPoint` because pushing a γ can expose a deeper join/union to push through.

Semi-join and intersect seeding must fire *after* `PushDownLeftSemiAntiJoin` (which is earlier), so
the semi-join has already been pushed as far as possible before we seed a γ off it.

---

## 5. Test plan

1. **Rule suites** (Catalyst, `PlanTest`): one suite per rule under
   `sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/optimizer/`
   - `PushPartialAggregateThroughJoinSuite` — the Fig 9 join split, single-side vs both-side,
     pre-condition rejection when an agg arg spans both inputs.
   - `PushPartialAggregateThroughUnionSuite` — Fig 10, `sum`/`min`/`max`/`count`.
   - `SeedPartialAggregateSuite` — group-by / semi-join / intersect seeding.
   - `PartialAggregateCostSuite` — threshold firing, `/dop` scaling, stage-single-γ.
2. **Stats suite**: extend `AggregateEstimationSuite` / new `PartialAggregateEstimationSuite`.
3. **End-to-end**: `SQLQueryTestSuite` TPC-DS `ao$` subsets (Q11, Q14, Q23, Q82/Q37-class) asserting
   the optimized plan contains a `PartialAggregate` below the join/union and results are unchanged.
4. **Correctness oracle**: run the TPCDS query set toggling the flag on/off; assert byte-identical
   row outputs (the "always optional" invariant).
5. **Gluten smoke**: execute a pushed-down plan on the Velox build; confirm partial+hashing runs
   natively (no fallback to JVM aggregate).

---

## 6. Ordering / dependency notes

- **Semi-join seeding depends on** `PushDownLeftSemiAntiJoin` having run first (as-is in the existing
  batch list — good).
- **Count rewrite**: `count(*) → sum(1)` must precede seeding (paper §4.1) so partial/final functions
  match. Place the rewrite guard inside `SeedPartialAggregate`.
- **Interaction with `RewriteDistinctAggregates`**: current physical path (`SparkStrategies`) handles
  DISTINCT separately (`functionsWithDistinct`); seeding must not push a γ across an un-rewritten
  DISTINCT aggregate — guard the join push-down when any agg arg is `isDistinct=true` whose rewrite
  hasn't happened.

---

## 7. Risks / open questions

1. **Cost-model honesty (main risk).** The paper's NDV-product estimate over-counts badly once
   γ keys span many columns (they concede this and patch only partition keys). Our Spark 5 has richer
   per-column stats (`ColumnStat`, histograms) — we can use `EstimationUtils` + actual distinct counts
   to materially improve `rr` accuracy vs. the paper. Worth deciding whether to use Spark's histogram
   NDV estimates or the paper's simple product. **Recommend: histogram-aware product, with `/dop`
   scaling — closer to Photon's behavior.**
2. **Physical `count(distinct)` + `Expand` interaction** — ensure we never push `γ` below an `Expand`
   that feeds a DISTINCT agg without the rewrite; else partial semantics break.
3. **Gluten fallback**: any `PartialAggregate` that Gluten can't vectorize must drop to the existing
   `ObjectHashAggregateExec`/`SortAggregateExec` path via `AggUtils` — the always-optional invariant
   means we can always remove it instead. No correctness risk; only perf.
4. **Fixed-point loop safety**: `PushPartialAggregateThroughJoin` introduces a `Project`; make sure
   it doesn't re-match its own output and re-push forever (use a guard on "didn't change" / depth).
5. **`threshold` sensitivity** (paper §7.4): 0.5→0.95 adds a few queries but little gain. Keep default
   0.5, expose the flag.

---

## 8. Deliverable checklist

- [x] `PartialAggregate` logical op + tree pattern
- [x] `SeedPartialAggregate` (group-by over Union + join, push-down rules)
- [x] Push-down rules: Union (done), Join (one-sided inner-equi, Fig 9 subset)
- [ ] Push-down rules: Join two-side split, Unary(Project/Filter/Expand/Window), semi-join/intersect seeding
- [x] `PartialAggregateCost` gate + stats handler in `BasicStatsPlanVisitor`
- [x] Physical lowering arm in `BasicOperators`
- [ ] `EliminatePartialAggregate` fallback (off by default)
- [x] Conf flags + `Optimizer` batch registration
- [x] Rule/estimation suites + SQLQueryTest TPCDS subset
- [ ] Gluten-native smoke test; flag-off correctness oracle