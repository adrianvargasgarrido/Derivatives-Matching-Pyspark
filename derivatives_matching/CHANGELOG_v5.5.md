# CHANGELOG — v5.5 (2026-03-11)

## Summary

v5.5 adds **three 1:1 traceability columns** to `all_matches` (and all downstream outputs).  
These columns answer the question: *"Which single Finstore trade is the best match for this Axis trade?"*  
Previously this required custom downstream logic. It is now a first-class output field.

---

## New Columns

| Column | Type | Values | Description |
|---|---|---|---|
| `match_rank` | `int` | 1, 2, 3 … | Rank of each Finstore leg per `axis_id`, ordered by `amount_diff ASC`, `fin_id ASC` (deterministic tie-break). Rank 1 = closest match. |
| `is_best_match` | `boolean` | `true` / `false` | `true` only for `match_rank == 1`. The recommended single axis → finstore mapping. |
| `match_type` | `string` | `"1:1"` / `"multi_leg"` | `"1:1"` when the axis has exactly one Finstore match row. `"multi_leg"` when it has 2 or more (multi-leg swap / structure). |

---

## Before / After Scenarios

### Scenario 1 — Clean 1:1 match (most trades)

**Before v5.5**
```
axis_id  fin_id   priority  amount_diff  description
A001     F101     2         0.00         OTC: TradeId exact
```
No way to distinguish this clean 1:1 from a multi-leg row without counting.

**After v5.5**
```
axis_id  fin_id   priority  amount_diff  match_rank  is_best_match  match_type
A001     F101     2         0.00         1           true           1:1
```
`is_best_match=true` + `match_type='1:1'` — unambiguous, no aggregation required.

---

### Scenario 2 — Multi-leg swap (1 Axis → 3 Finstore legs)

**Before v5.5**
```
axis_id  fin_id   amount_diff  description
A002     F201     500.00       OTC: InstrumentId + Counterparty
A002     F202     1200.00      OTC: InstrumentId + Counterparty
A002     F203     2800.00      OTC: InstrumentId + Counterparty
```
All three rows look identical in structure. Consumer must `groupBy(axis_id).min(amount_diff)` and self-join to find the best.

**After v5.5**
```
axis_id  fin_id   amount_diff  match_rank  is_best_match  match_type
A002     F201     500.00       1           true           multi_leg
A002     F202     1200.00      2           false          multi_leg
A002     F203     2800.00      3           false          multi_leg
```
`is_best_match=true` on F201 immediately identifies the primary match. F202/F203 are retained as secondary candidates.

---

### Scenario 3 — Filter to definitive 1:1 output (reporting / reconciliation)

**Before v5.5**
```python
# Required 2 extra steps downstream
legs = all_matches.groupBy("axis_id").agg(F.min("amount_diff").alias("min_diff"))
best = all_matches.join(legs, on="axis_id").filter(F.col("amount_diff") == F.col("min_diff"))
```

**After v5.5**
```python
# Single filter — no extra join
best = all_matches.filter(F.col("is_best_match"))
```
One filter gives a DataFrame with exactly one row per `axis_id` — the best finstore match.

---

### Scenario 4 — Isolate clean 1:1 vs multi-leg for separate reporting

**Before v5.5**
```python
# No built-in way — consumer had to count legs per axis themselves
legs_count = all_matches.groupBy("axis_id").count()
clean = all_matches.join(legs_count.filter(F.col("count") == 1), on="axis_id")
multi = all_matches.join(legs_count.filter(F.col("count") > 1), on="axis_id")
```

**After v5.5**
```python
clean = all_matches.filter(F.col("match_type") == "1:1")
multi = all_matches.filter(F.col("match_type") == "multi_leg")
```
Direct partition — no extra aggregation step.

---

### Scenario 5 — Greedy matches: all already 1:1 (no change to logic)

Greedy S1 and S2 produce exclusive 1:1 pairs via the iterative shrinking-pool algorithm. Their rows will always have:

```
match_rank     = 1
is_best_match  = true
match_type     = '1:1'
```

This is consistent with their algorithm — no greedy axis ever has more than one finstore.

---

### Scenario 6 — Verify traceability correctness (new summary block)

**Before v5.5** — consolidation cell output:
```
ℹ️  Within-rule many-to-many: 1,240 extra rows (51,240 total rows, 50,000 unique axis, ...)
```

**After v5.5** — consolidation cell output adds:
```
── 1:1 Traceability (v5.5) ──
  Axis with is_best_match=True: 50,000  (should == total_unique_axis)
  match_type='1:1':             47,800  (95.6%)
  match_type='multi_leg':        2,200  ( 4.4%)
  ℹ️  Filter on is_best_match=True for definitive 1 axis → 1 finstore mapping
```
Immediate visibility into what fraction of trades are clean 1:1 vs multi-leg.

---

### Scenario 7 — SQL query: inspect all legs for a multi-leg trade

```sql
-- Show all finstore candidates for a specific axis, ranked best-first
SELECT
    axis_id,
    fin_id,
    match_rank,
    is_best_match,
    match_type,
    amount_diff,
    description
FROM matched_all_base
WHERE axis_id = 'YOUR_AXIS_ID'
ORDER BY match_rank;
```

Previously required manual `row_number()` window — now pre-computed and stored.

---

### Scenario 8 — Reconciliation report: one row per axis

```sql
-- Definitive reconciliation output: exactly 1 row per axis_id
SELECT
    axis_id,
    fin_id,
    MatchLayer,
    description,
    amount_diff,
    match_type,
    verification_status
FROM matched_all_enriched
WHERE is_best_match = TRUE
ORDER BY axis_id;
```

---

### Scenario 9 — Escalation report: multi-leg trades requiring review

```sql
-- Trades where the algorithm found multiple finstore candidates
-- Sorted so rank-1 (best) appears first for each axis
SELECT
    axis_id,
    fin_id,
    match_rank,
    amount_diff,
    description
FROM matched_all_base
WHERE match_type = 'multi_leg'
ORDER BY axis_id, match_rank;
```

---

### Scenario 10 — `matched_all_base` column count updated

**Before v5.5:** ~15 columns  
**After v5.5:** ~18 columns (+ `match_rank`, `is_best_match`, `match_type`)

The three new columns are present in:
- `matched_all_base` (narrow Delta table)
- `matched_all_enriched` (wide Delta table, via `all_matches` join)
- `matched_brd_layer` — **not** tagged (BRD is saved from `brd_matches` directly, pre-consolidation)
- `matched_greedy_layer` — **not** tagged (same reason)

To get the tagged version for BRD-only or Greedy-only analysis, filter `matched_all_base` by `MatchLayer`.

---

## Implementation Details

### Where the columns are added

**Cell 41 — Final Consolidation:**

```python
# Union (unchanged)
_all_matches_raw = (
    brd_matches
    .unionByName(greedy1_matches)
    .unionByName(greedy2_matches)
)

# match_rank — row_number per axis_id, best amount_diff first
_w_rank = Window.partitionBy("axis_id").orderBy(
    F.col("amount_diff").asc_nulls_last(),
    F.col("fin_id").asc(),
)
_all_matches_ranked = _all_matches_raw.withColumn("match_rank", F.row_number().over(_w_rank))

# is_best_match — True for rank 1
_all_matches_ranked = _all_matches_ranked.withColumn("is_best_match", F.col("match_rank") == 1)

# match_type — count(*) per axis → '1:1' or 'multi_leg'
_w_legs = Window.partitionBy("axis_id")
_all_matches_ranked = _all_matches_ranked.withColumn("_n_legs", F.count("*").over(_w_legs))
_all_matches_ranked = _all_matches_ranked.withColumn(
    "match_type",
    F.when(F.col("_n_legs") == 1, F.lit("1:1")).otherwise(F.lit("multi_leg"))
).drop("_n_legs")

all_matches = _all_matches_ranked.persist(StorageLevel.MEMORY_AND_DISK)
```

### Why post-union (not per-layer)

- **BRD** produces M:M rows within a rule — `match_rank` must be computed across all finstore candidates simultaneously.
- **Greedy** rows are already 1:1, but they must rank consistently with BRD in the unified output.
- Computing rankings after the union gives a **single consistent definition** of "best match" across all layers.

### No schema changes to `_GREEDY_EMPTY_SCHEMA`

The three columns are added *after* the `unionByName`, so the per-layer schemas (`_GREEDY_EMPTY_SCHEMA`, `brd_matches`) are unchanged. No schema drift risk.

---

## Files Changed

| File | Change |
|---|---|
| `Derivatives_Matching_PySpark.ipynb` | Cell 41: consolidation rewritten with 3 traceability columns + summary block |
| `Derivatives_Matching_PySpark.ipynb` | Config cell: `RULE_VERSION = "v5.5-pyspark"` |
| `Derivatives_Matching_PySpark.ipynb` | Header markdown: table updated to `~18 cols`, date updated to 2026-03-11 |
| `Derivatives_Matching_PySpark.ipynb` | Save cell comment: updated to v5.5, `~18 cols` |

## Commit

```
3574e19  feat(v5.5): add 1:1 traceability columns (match_rank, is_best_match, match_type)
```

## Previous Version

See `CHANGELOG_v5.4.md` for the 9 resilience fixes (BRD checkpoint, greedy try/except, soft-fail verification, etc.).
