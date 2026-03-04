# Changelog — v4.6: Many-to-Many Matching (Pandas Parity)

**Date:** 2025-06-24  
**Previous version:** v4.5 (commit `4ff12fe`)  
**Branch:** `main`

---

## Summary

Changed the BRD match resolution from **many-to-one** (each axis → 1 best fin,
same fin reusable) to **many-to-many** (one axis can match multiple finstores),
fully replicating the original Pandas `pd.merge(how="inner")` behaviour.

In v4.5, `resolve_matches()` applied a window dedup (best fin per axis) to ALL
layers, including BRD. This meant that if 1 axis trade matched 3 finstore trades
via different BRD rules, only the best match was kept. In the Pandas notebook,
`pd.merge(how="inner")` preserves **all** matching rows — a true many-to-many
join.

Now:

| Layer | v4.5 | v4.6 (current) | Pandas equivalent |
|---|---|---|---|
| **BRD** | `resolve_matches()` → 1 row per axis | Keep ALL `inner join` rows | `pd.merge(how="inner")` |
| **Greedy S1** | `resolve_matches()` → 1 row per axis | `resolve_matches()` → 1 row per axis (unchanged) | `nsmallest(1, "amount_diff")` |
| **Greedy S2** | `resolve_matches()` → 1 row per axis | `resolve_matches()` → 1 row per axis (unchanged) | `nsmallest(1, "amount_diff")` |

Key metric changes:
- **Match rate** is now based on **unique axis count** (`.distinct().count()`), not total row count.
- **Total match rows** may exceed total unique axis IDs (many-to-many edges).
- Pool removal uses `.distinct()` on axis/fin IDs before anti-join.

---

## Changes Made (10 locations)

### 1. Pipeline Overview Markdown

**Before:**
```markdown
Resolve matches via window ranking
```

**After:**
```markdown
Keep all many-to-many matches (Pandas `pd.merge(inner)` parity)
```

---

### 2. Section 10 Markdown — Title & Description

**Before:**
```markdown
### 🔹 Section 10: Resolve Matches — Best Fin per Axis (Many-to-One)
…
resolve_matches()  window ranking (many-to-one allowed)
```

**After:**
```markdown
### 🔹 Section 10: BRD Match Resolution — Many-to-Many (Pandas Parity)

For BRD rules, **all** inner-join results are kept — no window dedup.
`resolve_matches()` is used **only** by Greedy Strategy 1 & 2, where it
replicates the Pandas `nsmallest(1, "amount_diff")` behaviour.
```

---

### 3. Cell 23 — BRD Resolution (THE CORE CHANGE)

**Before:**
```python
# --- BRD Resolved Matches ------------------------------------------------
print("Resolving matches (best fin per axis — many-to-one allowed)...")
brd_matches = resolve_matches(candidates_layer1)
brd_matches.cache()

brd_match_count = brd_matches.count()
brd_match_rate  = brd_match_count / ORIGINAL_AXIS_COUNT * 100

# Unique fin_ids matched (may be < brd_match_count due to many-to-one)
brd_unique_fin = brd_matches.select("fin_id").distinct().count()

print(f"\n--- BRD Layer Results ---")
print(f"Axis matched:        {brd_match_count:,}")
print(f"Unique Finstore used:{brd_unique_fin:,}  (many-to-one: {brd_match_count - brd_unique_fin:,} fin reuses)")
print(f"Match rate:          {brd_match_rate:.2f}%")
```

**After:**
```python
# --- BRD Resolved Matches ------------------------------------------------
# Many-to-many: keep ALL inner-join edges (Pandas pd.merge parity).
# resolve_matches() is used ONLY by Greedy layers below.
print("BRD matches — keeping all many-to-many edges (Pandas parity)...")
brd_matches = candidates_layer1
brd_matches.cache()

brd_match_rows  = brd_matches.count()           # total edges (may be > unique axis)
brd_unique_axis = brd_matches.select("axis_id").distinct().count()
brd_unique_fin  = brd_matches.select("fin_id").distinct().count()
brd_match_rate  = brd_unique_axis / ORIGINAL_AXIS_COUNT * 100

print(f"\n--- BRD Layer Results (Many-to-Many) ---")
print(f"Match rows (edges):   {brd_match_rows:,}")
print(f"Unique Axis matched:  {brd_unique_axis:,}")
print(f"Unique Finstore used: {brd_unique_fin:,}")
print(f"Match rate (axis):    {brd_match_rate:.2f}%")
```

**Key difference:** `brd_matches = candidates_layer1` (no `resolve_matches()` call).
The `resolve_matches()` function definition is unchanged but is now marked with:
```python
# Used ONLY by Greedy layers (S1, S2) — NOT by BRD.
```

---

### 4. Section 11b Markdown — Greedy Resolution Description

**Before:**
```markdown
resolve_matches()  window ranking (many-to-one allowed)
```

**After:**
```markdown
resolve_matches() window dedup — best fin per axis (Greedy only).
Matches Pandas `nsmallest(1, "amount_diff")`.
BRD layer keeps ALL many-to-many edges (no resolve_matches call).
```

---

### 5. Cell 26 — Pool Removal (Anti-Join)

**Before:**
```python
matched_axis_ids = brd_matches.select("axis_id")
matched_fin_ids  = brd_matches.select("fin_id")
```

**After:**
```python
matched_axis_ids = brd_matches.select("axis_id").distinct()
matched_fin_ids  = brd_matches.select("fin_id").distinct()
```

**Reason:** With many-to-many, `brd_matches` can have duplicate axis_id and
fin_id values. The `.distinct()` ensures the anti-join removes each ID exactly
once (idempotent), preventing duplicate rows in the remaining pool.

---

### 6. Cell 34 — Consolidation

**Before:**
```python
all_matches = (
    brd_matches
    .unionByName(greedy1_matches)
    .unionByName(greedy2_matches)
    .dropDuplicates(["axis_id"])   # each axis trade can only have one match
    # NOTE: NO dropDuplicates(["fin_id"]) — many-to-one is intentional
)

# ...
actual_count = all_matches.count()
total_unique_fin = all_matches.select("fin_id").distinct().count()

# Cross-layer axis uniqueness assertion
if actual_count != total_matched:
    leakage = total_matched - actual_count
    print(f"⚠️  WARNING: {leakage:,} cross-layer duplicate axis_ids were removed.")
else:
    print("✅ Cross-layer axis uniqueness check PASSED")
```

**After:**
```python
all_matches = (
    brd_matches
    .unionByName(greedy1_matches)
    .unionByName(greedy2_matches)
    # NOTE: NO dropDuplicates — many-to-many edges are intentional for BRD.
    # Greedy layers already have 1 row per axis via resolve_matches().
)

total_match_rows  = all_matches.count()
total_unique_axis = all_matches.select("axis_id").distinct().count()
total_unique_fin  = all_matches.select("fin_id").distinct().count()

# total_matched = unique axis count (for match rate)
total_matched = brd_unique_axis + total_greedy

# Cross-layer check: BRD and Greedy axis sets should not overlap
brd_axis_set     = brd_matches.select("axis_id").distinct()
greedy_axis_set  = greedy1_matches.select("axis_id").unionByName(
                       greedy2_matches.select("axis_id")).distinct()
brd_greedy_overlap = brd_axis_set.join(greedy_axis_set, "axis_id", "inner").count()

if brd_greedy_overlap > 0:
    print(f"⚠️  WARNING: {brd_greedy_overlap:,} axis_ids appear in BOTH BRD and Greedy.")
else:
    print("✅ Pool isolation check PASSED — no axis_id in both BRD and Greedy.")
```

**Changes:**
- Removed `dropDuplicates(["axis_id"])` — BRD rows are many-to-many by design.
- New variables: `total_match_rows`, `total_unique_axis`, `total_unique_fin`.
- `total_matched` is now the sum of unique axis counts per layer.
- Cross-layer check changed from row count comparison to BRD/Greedy axis overlap.

---

### 7. Cell 35 — Section 15a Verification Markdown

**Before:**
```markdown
Check 1: axis uniqueness across layers
Check 2: matched vs unmatched overlap
Check 3: arithmetic total matches expected count
```

**After:**
```markdown
Check 1: BRD / Greedy pool isolation (no axis in both layers)
Check 2: Matched vs unmatched overlap (uses .distinct() axis IDs)
Check 3: Unique matched axis + unmatched axis = ORIGINAL_AXIS_COUNT
```

---

### 8. Cell 36 — Verification Assertions

**Before:**
```python
# Check 1 — axis uniqueness
axis_dup_count = all_matches.groupBy("axis_id").count().filter(F.col("count") > 1).count()
assert axis_dup_count == 0, f"FAIL: {axis_dup_count} axis_ids matched more than once"

# Check 2 — matched vs unmatched overlap
matched_axis_set   = all_matches.select("axis_id")
overlap = matched_axis_set.join(unmatched_axis.select("axis_id"), "axis_id", "inner").count()
assert overlap == 0, f"FAIL: {overlap} axis_ids in both matched and unmatched"

# Check 3 — total check
assert total_matched + unmatched_count == ORIGINAL_AXIS_COUNT
```

**After:**
```python
# Check 1 — BRD/Greedy pool isolation
assert brd_greedy_overlap == 0, \
    f"FAIL: {brd_greedy_overlap} axis_ids in both BRD and Greedy layers"

# Check 2 — matched vs unmatched overlap (distinct axis IDs)
matched_axis_set = all_matches.select("axis_id").distinct()
overlap = matched_axis_set.join(
    unmatched_axis.select("axis_id"), "axis_id", "inner"
).count()
assert overlap == 0, f"FAIL: {overlap} axis_ids in both matched and unmatched"

# Check 3 — total check (unique axis)
assert total_matched + unmatched_count == ORIGINAL_AXIS_COUNT, \
    f"FAIL: {total_matched} + {unmatched_count} ≠ {ORIGINAL_AXIS_COUNT}"
```

---

### 9. Accuracy Report Cell (Section 22)

**Variable renames:**
| Before | After | Meaning |
|---|---|---|
| `brd_match_count` | `brd_match_rows` | Total edges (many-to-many) |
| — | `brd_unique_axis` | Unique axis IDs matched by BRD |
| `total_matched` (row count) | `total_match_rows` | Total edges across all layers |
| — | `total_unique_axis` | Unique axis IDs across all layers |

**Updated semantics section:**
```
MATCHING SEMANTICS  (v4.6 — Many-to-Many Pandas Parity)
  BRD layer: all inner-join edges kept (many-to-many)   : Pandas pd.merge parity
  Greedy layers: best fin per axis (window dedup)        : Pandas nsmallest parity
  Match rate based on unique axis count                  : .distinct().count()
  Pool removal uses .distinct() axis/fin IDs             : idempotent anti-join
```

**Rate calculations:**
- All rates now use `brd_unique_axis` (not `brd_match_rows`) as numerator.
- Output table row counts use `total_match_rows`.

---

### 10. Delta Save / Print Cells

**Before:**
```python
print(f"BRD matches: {brd_match_count:,}")
print(f"Total matched: {total_matched:,}")
```

**After:**
```python
print(f"BRD match rows (edges): {brd_match_rows:,}")
print(f"BRD unique axis:        {brd_unique_axis:,}")
print(f"Total match rows:       {total_match_rows:,}")
print(f"Total unique axis:      {total_unique_axis:,}")
```

---

## Variable Reference

### New Variables (v4.6)

| Variable | Scope | Description |
|---|---|---|
| `brd_match_rows` | BRD layer | Total match edges (rows) from BRD many-to-many |
| `brd_unique_axis` | BRD layer | Count of distinct axis_ids matched by BRD |
| `brd_unique_fin` | BRD layer | Count of distinct fin_ids matched by BRD |
| `total_match_rows` | Consolidation | Total edges across BRD + Greedy S1 + Greedy S2 |
| `total_unique_axis` | Consolidation | Distinct axis_ids across all layers |
| `total_unique_fin` | Consolidation | Distinct fin_ids across all layers |

### Removed / Replaced Variables

| Removed | Replaced by | Notes |
|---|---|---|
| `brd_match_count` | `brd_match_rows` + `brd_unique_axis` | Split into rows vs unique axis |
| `dropDuplicates(["axis_id"])` in consolidation | Removed | BRD many-to-many by design |

---

## What Did NOT Change

- **`resolve_matches()` function definition** — unchanged (still exists, used by Greedy S1 & S2)
- **BRD rule definitions** (15 rules) — unchanged
- **`build_candidates_for_rule()`** — unchanged
- **Bronze / Silver / Core splits** — unchanged
- **Greedy candidate generation** (S1 counterparty, S2 bucket) — unchanged
- **Greedy call sites** — still use `resolve_matches()` (1 row per axis)
- **Enrichment / DQ / Save logic** — unchanged
- **Section 23 verdict** — unchanged
- **Exclusion logic** (SOPHIS/DELTA1 applied to Axis only) — unchanged

---

## Migration from v4.5

If reverting to v4.5 (many-to-one for all layers):
1. Replace `brd_matches = candidates_layer1` with `brd_matches = resolve_matches(candidates_layer1)`
2. Remove `brd_match_rows`, `brd_unique_axis`, `brd_unique_fin` — use `brd_match_count` instead
3. Remove `.distinct()` from pool removal
4. Restore `dropDuplicates(["axis_id"])` in consolidation
5. Restore row-count-based verification checks
