# Changelog — v4.5: Many-to-One Matching (Pandas Parity)

**Date:** 2026-03-04  
**Commit:** v4.5  
**Previous version:** v4.4 (commit `c878605`)  
**Branch:** `main`

---

## Summary

Changed the match resolution from **strict 1-to-1** (one axis ↔ one finstore) to
**many-to-one** (multiple axis trades → same finstore record), replicating the
original Pandas `pd.merge(inner)` behaviour.

Previously, the `resolve_one_to_one()` function used 3 window passes to enforce
that each `axis_id` AND each `fin_id` appeared at most once. This meant that if
3 axis trades shared the same `DerivedSophisId` and matched the same finstore
`fissnumber`, only 1 of the 3 was kept — the other 2 fell to unmatched/greedy.

Now, `resolve_matches()` uses a single window pass: for each `axis_id`, pick the
best `fin_id`. Multiple axis trades can match the **same** finstore record.

---

## Changes Made (7 locations)

### 1. Section 10 Markdown — Title & Description

**File:** `Derivatives_Matching_PySpark.ipynb` — markdown cell before resolve function

**Before:**
```markdown
### 🔹 Section 10: Resolve 1-to-1 Matches (Waterfall via Window Ranking)

This is the **key performance optimisation**. Instead of 15 iterative
"join → remove matched → repeat" passes, we do **two window passes**:

1. **Best Finstore per Axis:** for each `axis_id`, pick the candidate
   with lowest `priority` (then smallest `amount_diff`, then smallest `fin_id` for stability)
2. **Best Axis per Finstore:** from the reduced set, for each `fin_id`,
   pick the candidate with lowest `priority` (same tiebreakers)

This replicates waterfall removal + prevents double-matching.
```

**After:**
```markdown
### 🔹 Section 10: Resolve Matches — Best Fin per Axis (Many-to-One)

This is the **key performance optimisation**. Instead of 15 iterative
"join → remove matched → repeat" passes, we do a **single window pass**:

1. **Best Finstore per Axis:** for each `axis_id`, pick the candidate
   with lowest `priority` (then highest `key_strength`, smallest `amount_diff`,
   then smallest `fin_id` for stability)

Multiple Axis trades sharing the same key **can** match the same Finstore
record (many-to-one), replicating the Pandas `pd.merge(inner)` behaviour.
```

---

### 2. Core Function — `resolve_one_to_one` → `resolve_matches`

**File:** `Derivatives_Matching_PySpark.ipynb` — code cell (Section 10)

**Before:**
```python
# ============================================================
# 1-to-1 RESOLUTION via Window Ranking
# ============================================================

def resolve_one_to_one(candidates: DataFrame) -> DataFrame:
    """
    Enforce 1-to-1 matching using iterative window ranking.
    
    Pass 1: For each axis_id, pick best fin_id
    Pass 2: For each fin_id, pick best axis_id from Pass 1 result
    Pass 3: (Safety) Re-check axis uniqueness after Pass 2
    
    Best Practice §2E: iterative resolution with stable tiebreakers.
    """
    has_key_strength = "key_strength" in candidates.columns
    ordering = [F.col("priority").asc()]
    if has_key_strength:
        ordering.append(F.col("key_strength").desc())
    ordering.append(F.col("amount_diff").asc_nulls_last())

    # Pass 1: best fin per axis
    w_axis = Window.partitionBy("axis_id").orderBy(*ordering, F.col("fin_id").asc())
    best_per_axis = (
        candidates
        .withColumn("_rn_axis", F.row_number().over(w_axis))
        .filter(F.col("_rn_axis") == 1)
        .drop("_rn_axis")
    )

    # Pass 2: best axis per fin (prevents fin reuse)
    w_fin = Window.partitionBy("fin_id").orderBy(*ordering, F.col("axis_id").asc())
    pass2 = (
        best_per_axis
        .withColumn("_rn_fin", F.row_number().over(w_fin))
        .filter(F.col("_rn_fin") == 1)
        .drop("_rn_fin")
    )

    # Pass 3: safety — re-enforce axis uniqueness after fin dedup
    w_axis_final = Window.partitionBy("axis_id").orderBy(*ordering, F.col("fin_id").asc())
    resolved = (
        pass2
        .withColumn("_rn_final", F.row_number().over(w_axis_final))
        .filter(F.col("_rn_final") == 1)
        .drop("_rn_final")
    )

    return resolved
```

**After:**
```python
# ============================================================
# MATCH RESOLUTION — Best fin per axis (many-to-one allowed)
# ============================================================
# Replicates the Pandas pd.merge(inner) behaviour:
#   • Each axis_id gets exactly 1 match (its best fin_id).
#   • Multiple axis_ids MAY match the SAME fin_id.

def resolve_matches(candidates: DataFrame) -> DataFrame:
    """
    For each axis_id, pick the single best fin_id candidate.

    Ranking criteria (in order):
        1. lowest priority  (highest-confidence rule wins)
        2. highest key_strength  (more join keys = stronger match)
        3. smallest amount_diff  (closest amount = best tiebreaker)
        4. smallest fin_id  (deterministic / stable)

    Unlike the previous resolve_one_to_one, this does NOT deduplicate
    on fin_id — a single Finstore record can be matched by multiple
    Axis records, exactly like the Pandas notebook.
    """
    has_key_strength = "key_strength" in candidates.columns
    ordering = [F.col("priority").asc()]
    if has_key_strength:
        ordering.append(F.col("key_strength").desc())
    ordering.append(F.col("amount_diff").asc_nulls_last())

    # Single pass: best fin per axis
    w_axis = Window.partitionBy("axis_id").orderBy(*ordering, F.col("fin_id").asc())
    resolved = (
        candidates
        .withColumn("_rn_axis", F.row_number().over(w_axis))
        .filter(F.col("_rn_axis") == 1)
        .drop("_rn_axis")
    )

    return resolved
```

---

### 3. BRD Call Site + Results Print

**Before:**
```python
print("Resolving 1-to-1 matches...")
brd_matches = resolve_one_to_one(candidates_layer1)

# ...
print(f"Unique Axis matched: {brd_match_count:,}")
```

**After:**
```python
print("Resolving matches (best fin per axis — many-to-one allowed)...")
brd_matches = resolve_matches(candidates_layer1)

# ...
# Unique fin_ids matched (may be < brd_match_count due to many-to-one)
brd_unique_fin = brd_matches.select("fin_id").distinct().count()

print(f"Axis matched:        {brd_match_count:,}")
print(f"Unique Finstore used:{brd_unique_fin:,}  (many-to-one: {brd_match_count - brd_unique_fin:,} fin reuses)")
```

---

### 4. Greedy Strategy 1 — Call Site

**Before:**
```python
# Resolve 1-to-1
greedy1_matches = resolve_one_to_one(greedy1_candidates)
```

**After:**
```python
# Resolve best-fin-per-axis (many-to-one allowed)
greedy1_matches = resolve_matches(greedy1_candidates)
```

---

### 5. Greedy Strategy 2 — Call Site

**Before:**
```python
# Resolve 1-to-1
greedy2_matches = resolve_one_to_one(greedy2_candidates)
```

**After:**
```python
# Resolve best-fin-per-axis (many-to-one allowed)
greedy2_matches = resolve_matches(greedy2_candidates)
```

---

### 6. Consolidation — `dropDuplicates` & Cross-Layer Assertion

**Before:**
```python
all_matches = (
    brd_matches
    .unionByName(greedy1_matches)
    .unionByName(greedy2_matches)
    .dropDuplicates(["axis_id"])   # each axis trade can only have one match
    .dropDuplicates(["fin_id"])    # each fin trade can only have one match
)

# ...
dup_check = all_matches.agg(
    F.sum(F.when(F.col("axis_id").isNull(), 1).otherwise(0)).alias("null_axis"),
    F.sum(F.when(F.col("fin_id").isNull(), 1).otherwise(0)).alias("null_fin"),
).collect()[0]

actual_count = all_matches.count()
if actual_count != total_matched:
    leakage = total_matched - actual_count
    print(f"⚠️  WARNING: {leakage:,} cross-layer duplicate IDs were removed by deduplication guard.")
else:
    print("✅ Cross-layer uniqueness check PASSED — no axis_id or fin_id matched twice.")
```

**After:**
```python
all_matches = (
    brd_matches
    .unionByName(greedy1_matches)
    .unionByName(greedy2_matches)
    .dropDuplicates(["axis_id"])   # each axis trade can only have one match
    # NOTE: NO dropDuplicates(["fin_id"]) — many-to-one is intentional
)

# ...
# Cross-layer axis uniqueness assertion
# fin_id uniqueness is NOT checked — many-to-one is expected.
actual_count = all_matches.count()
if actual_count != total_matched:
    leakage = total_matched - actual_count
    print(f"⚠️  WARNING: {leakage:,} cross-layer duplicate axis_ids were removed.")
else:
    print("✅ Cross-layer axis uniqueness check PASSED — no axis_id matched in more than one layer.")

# Fin reuse summary (informational — many-to-one is expected)
total_unique_fin = all_matches.select("fin_id").distinct().count()
fin_reuses = actual_count - total_unique_fin
if fin_reuses > 0:
    print(f"ℹ️  Many-to-one: {fin_reuses:,} Finstore records matched by >1 Axis trade (expected).")
else:
    print("ℹ️  All matches are 1-to-1 (no Finstore record matched by multiple Axis trades).")
```

---

### 7. Section 15a Verification Markdown + Section 22 Accuracy Report

**Section 15a markdown** — added note:
```markdown
> **Note**: `fin_id` uniqueness is NOT asserted — many-to-one matching
> (multiple Axis trades → same Finstore record) is expected and mirrors
> the Pandas notebook behaviour.
```

**Section 22 accuracy report** — updated dedup guard section:

**Before:**
```
CROSS-LAYER DEDUPLICATION GUARD
  Each axis_id appears in exactly 1 match layer  : enforced via dropDuplicates
  Each fin_id  appears in exactly 1 match layer  : enforced via dropDuplicates
  Runtime assertion (actual vs arithmetic count) : verified in Section 15
```

**After:**
```
MATCHING SEMANTICS
  Each axis_id appears in exactly 1 match row    : enforced via dropDuplicates
  Many-to-one (multiple axis → same fin_id)       : ALLOWED (Pandas parity)
  Runtime assertion (actual vs arithmetic count)  : verified in Section 15
```

---

### Minor Reference Updates

| Location | Before | After |
|---|---|---|
| Candidate generation comment | `resolve_one_to_one` | `resolve_matches` |
| Candidate persist comment | `resolve_one_to_one reads it` | `resolve_matches reads it` |
| Greedy markdown | `1-to-1 resolution via the same window ranking approach` | `Best-fin-per-axis resolution via the same window ranking (many-to-one allowed)` |

---

## Exclusion Logic Review

Also audited during this version: the SOPHIS/DELTA1 scope exclusion is
**correctly applied to Axis only** (not Finstore). This is identical to the
Pandas notebook behaviour. If excluded system names appear in the output,
they come from the **Finstore side** (e.g. `SourceSystemName_Finstore`),
which is expected — Finstore records are never scope-filtered in either
implementation.

---

## What Did NOT Change

- **BRD rule definitions** (15 rules) — unchanged
- **`build_candidates_for_rule()`** — unchanged
- **Bronze / Silver / Core splits** — unchanged
- **Greedy candidate generation** (S1 counterparty, S2 bucket) — unchanged
- **Anti-join unmatched pools** — unchanged (still removes matched axis_ids AND fin_ids)
- **Enrichment / DQ / Save logic** — unchanged
- **Section 15a verification code** — unchanged (3 assertion checks still run)
- **Section 23 verdict** — unchanged
