# Changelog — v4.7: Waterfall Parity (Best Rule per Axis)

**Date:** 2026-03-05  
**Previous version:** v4.6 (commit `aa63e2d`)  
**Branch:** `main`

---

## Problem — v4.6 Cartesian Explosion

v4.6 set `brd_matches = candidates_layer1` (the raw union of ALL 15 BRD rules).
PySpark generates candidates by joining each rule against the **full** axis and
finstore pools (no waterfall removal). This means:

- Axis A matches via Rule 1 AND Rule 3 AND Rule 5 (same or different finstores)
- Finstore F matches via Rule 1 AND Rule 7 (with different axes)

All these cross-rule duplicates were kept, causing **3 M axis → 198 M rows**.

## Root Cause — Pandas Waterfall vs. PySpark Parallel

**Pandas** runs rules sequentially with **pool removal after each rule**:
```
Rule 1: merge(axis_pool, fin_pool) → matched → remove matched from pools
Rule 2: merge(remaining_axis, remaining_fin) → matched → remove again
...
Rule 15: merge(tiny_remaining, remaining_fin)
```
Each axis matches via **at most one rule** (its first hit removes it from the pool).
Within that rule, `pd.merge(inner)` CAN produce multiple rows (many-to-many).

**PySpark** runs all 15 rules against the full pools in parallel, then unions.
Without waterfall removal, the same axis appears in candidates from multiple rules.

## Fix — Best Priority per Axis (v4.7)

For each `axis_id`, find its **best (lowest) priority** across all candidate rows,
then keep **only** the rows at that priority level:

```python
w_best = Window.partitionBy("axis_id")
brd_matches = (
    candidates_layer1
    .withColumn("_best_priority", F.min("priority").over(w_best))
    .filter(F.col("priority") == F.col("_best_priority"))
    .drop("_best_priority")
)
```

This gives:
- **Across rules**: each axis keeps only its highest-priority (lowest number) rule → waterfall parity
- **Within a rule**: all finstore matches preserved (1 axis → N finstores if key hits N records) → `pd.merge(inner)` parity
- **Cross-rule explosion eliminated**: axis matching rules 1, 3, 5 now keeps only rule 1's rows

---

## Changes Made (8 locations)

### 1. Pipeline Overview Markdown (Cell 17)

**Before:**
```
3. Keep all many-to-many matches (Pandas pd.merge(inner) parity)
```

**After:**
```
3. Resolve: best rule per axis (waterfall parity), keeping all within-rule matches
```

---

### 2. Section 10 Markdown (Cell 22)

**Before:**
```
### Section 10: BRD Match Resolution — Many-to-Many (Pandas Parity)
…all candidate edges are kept — no window dedup…
```

**After:**
```
### Section 10: BRD Match Resolution — Waterfall Parity (Best Rule per Axis)
…for each axis_id, only the rows from its best (lowest) priority rule…
```

---

### 3. Cell 23 — BRD Resolution (THE CORE FIX)

**Before (v4.6):**
```python
brd_matches = candidates_layer1  # keep ALL — caused 198M explosion
```

**After (v4.7):**
```python
# Step 1: For each axis_id, find the best (lowest) priority it matched
w_best_priority = Window.partitionBy("axis_id")
brd_matches = (
    candidates_layer1
    .withColumn("_best_priority", F.min("priority").over(w_best_priority))
    .filter(F.col("priority") == F.col("_best_priority"))
    .drop("_best_priority")
)
```

This is the key change. The window function finds the minimum priority per axis,
then keeps only rows at that priority level. All finstore matches within that
best rule are preserved (within-rule many-to-many).

---

### 4. Cell 26 — Pool Removal Comments

Updated comment:
```python
# brd_matches may have within-rule many-to-many, so we use DISTINCT
# axis_id / fin_id for pool removal
```

`.distinct()` on axis/fin IDs unchanged (still needed for within-rule many-to-many).

---

### 5. Cell 34 — Consolidation Comments

```python
# BRD layer: best-rule per axis, all fin matches within that rule
#   (1 axis can appear N times if its best rule hit N finstores)
# Cross-rule duplication is already prevented by the best-priority filter.
```

Logic unchanged — still no `dropDuplicates`, same variables.

---

### 6. Cell 35 — Verification Markdown

Updated note:
```
axis_id can appear multiple times within BRD matches
if its best rule produced multiple finstore hits (within-rule many-to-many).
```

---

### 7. Cell 52 — Summary Report

```
- Candidate edges: BRD keeps best-rule per axis (waterfall parity), Greedy uses window ranking
```

---

### 8. Cell 56 — Accuracy Report Semantics

```
MATCHING SEMANTICS  (v4.7 — Waterfall Parity)
  BRD: best rule per axis, all fin matches within rule    : Pandas waterfall
  Greedy layers: 1 axis → 1 best fin (window dedup)      : Pandas nsmallest
```

---

## Comparison: v4.5 → v4.6 → v4.7

| Aspect | v4.5 | v4.6 (broken) | v4.7 (current) | Pandas |
|---|---|---|---|---|
| BRD resolution | `resolve_matches()` → 1 row per axis | Raw candidates (ALL rows) | Best priority per axis, all rows within | Waterfall: 1 rule per axis, all merge rows |
| Cross-rule duplication | Prevented (window dedup) | **NOT prevented → 198M explosion** | Prevented (best-priority filter) | Prevented (pool removal) |
| Within-rule many-to-many | ❌ collapsed to 1 row | ✅ preserved | ✅ preserved | ✅ preserved |
| Expected row count | ~= unique axis count | 60× axis count | ~= unique axis + modest within-rule dupes | ~= unique axis + modest within-rule dupes |

---

## What Did NOT Change (from v4.6)

- **`resolve_matches()` function** — still exists, still used only by Greedy S1 & S2
- **Pool removal with `.distinct()`** — unchanged
- **Consolidation structure** — no `dropDuplicates`, same variables
- **Verification checks** — same 3 checks (pool isolation, overlap, count)
- **Accuracy report layout** — same, just semantics label updated
- **All greedy logic** — unchanged
- **All BRD rule definitions** — unchanged
- **`build_candidates_for_rule()`** — unchanged

---

## Migration

If reverting to v4.5 (strict 1 row per axis across ALL layers):
1. Replace the best-priority filter with `brd_matches = resolve_matches(candidates_layer1)`
2. Remove `brd_match_rows`, `brd_unique_axis` — use `brd_match_count`
3. Remove `.distinct()` from pool removal
4. Restore `dropDuplicates(["axis_id"])` in consolidation
