# CHANGELOG v5.1 — Greedy False-Positive Fix

**Date:** 5 March 2026  
**Version:** v5.0-pyspark → v5.1-pyspark  
**Branch:** main  
**Author:** Adrian Vargas  

---

## Summary

Fixed greedy matching logic that was producing ~19% inflated match rate due to 4 root causes:
1. **No fin_id exclusivity** — same Finstore record matched by multiple Axis trades
2. **No SourceSystemName blocking** — cross-system false positives
3. **No minimum amount threshold** — near-zero amounts trivially matched
4. **Amount-only matching (Strategy 2)** — too loose without system constraint

Added 6th invariant check (greedy fin_id exclusivity) and quality diagnostics cell.

---

## Section-by-Section Change Map

| # | Section | Cell | Status | Change Type |
|---|---------|------|--------|-------------|
| 1 | Spark Session & Imports | Cell 3 | ✅ **UNCHANGED** | — |
| 2 | Scope Configuration | Cell 4 | ⚠️ **MODIFIED** | 2 new params + version bump |
| 3 | Spark Performance Tuning | Cell 6 | ✅ **UNCHANGED** | — |
| 4 | Matching Rules (P1–P15) | Cell 8 | ✅ **UNCHANGED** | — |
| 5 | Data Ingestion | Cell 10 | ✅ **UNCHANGED** | — |
| 6 | Composite Keys | Cell 12 | ✅ **UNCHANGED** | — |
| 7 | Derived Columns | Cell 14 | ✅ **UNCHANGED** | — |
| 8 | Core/Wide Split | Cell 16 | ✅ **UNCHANGED** | — |
| 9 | safe_rule_join() | Cell 19 | ✅ **UNCHANGED** | — |
| 10 | Sequential Waterfall Loop | Cell 21 | ✅ **UNCHANGED** | — |
| 10a | Diagnostics Dashboard | Cell 23 | ✅ **UNCHANGED** | — |
| 10b | BRD Resolution + resolve_matches() | Cell 25 | ⚠️ **MODIFIED** | `resolve_matches()` rewritten |
| 10c | POC Match Validation | Cell 27 | ✅ **UNCHANGED** | — |
| 11 | Unmatched Pools for Greedy | Cell 30 | ⚠️ **MODIFIED** | MIN_GREEDY_AMOUNT filter added |
| 12 | Greedy Strategy 1 (markdown) | Cell 31 | ⚠️ **MODIFIED** | Title updated |
| 12 | Greedy Strategy 1 (code) | Cell 32 | ⚠️ **MODIFIED** | System equi-join + 1:1 resolution |
| 13 | Greedy Strategy 2 (markdown) | Cell 33 | ⚠️ **MODIFIED** | Title updated |
| 13 | Greedy Strategy 2 (code) | Cell 34 | ⚠️ **MODIFIED** | System equi-join + 1:1 resolution |
| 14 | Greedy Layer Summary | Cell 36 | ⚠️ **MODIFIED** | Updated labels + greedy rate % |
| 14a | Greedy Quality Diagnostics | Cell 37 | 🆕 **NEW CELL** | Entirely new diagnostic cell |
| 15 | Final Consolidation | Cell 39 | ⚠️ **MODIFIED** | Comments updated |
| 15a | Verification (markdown) | Cell 40 | ⚠️ **MODIFIED** | 5→6 invariants |
| 15a | Verification (code) | Cell 41 | ⚠️ **MODIFIED** | Added CHECK 6 |
| 16–21 | Save Results / Delta Write | Cells 42–57 | ✅ **UNCHANGED** | — |
| 22a | Accuracy Report | Cell 59 | ✅ **UNCHANGED** | — |
| 22b | POC Summary Report (markdown) | Cell 62 | ⚠️ **MODIFIED** | v5.0→v5.1 |
| 22b | POC Summary Report (code) | Cell 63 | ⚠️ **MODIFIED** | Header + semantics section |
| 23 | Verdict | Cell 65 | ✅ **UNCHANGED** | — |

---

## Detailed Changes (Copy-Paste Ready)

---

### CHANGE 1: Scope Configuration (Cell 4)

**What changed:** Added 2 new parameters (`MIN_GREEDY_AMOUNT`, `GREEDY_MAX_ITER`) and bumped `RULE_VERSION` to v5.1.

**Find this block and replace:**

```python
# OLD (v5.0):
# Greedy matching parameters
GREEDY_TOLERANCE_PCT = 0.01     # 1% for Strategy 1 (Amount + Counterparty)
STRICT_TOLERANCE_PCT = 0.001    # 0.1% for Strategy 2 (Amount only)
BUCKET_SIZE = 1000              # Amount bucket size for Strategy 2 blocking
```

**Replace with:**

```python
# NEW (v5.1):
# Greedy matching parameters
GREEDY_TOLERANCE_PCT = 0.01     # 1% for Strategy 1 (Amount + Counterparty)
STRICT_TOLERANCE_PCT = 0.001    # 0.1% for Strategy 2 (Amount only)
BUCKET_SIZE = 1000              # Amount bucket size for Strategy 2 blocking
MIN_GREEDY_AMOUNT = 100.0       # Minimum |amount| for greedy eligibility — prevents near-zero false positives
GREEDY_MAX_ITER = 5             # Max stable-marriage iterations for 1:1 enforcement
```

**Also in the same cell, find and replace:**

```python
# OLD:
RULE_VERSION = "v5.0-pyspark"                         # v5.0: safe_rule_join, sequential waterfall, P15 fix

# NEW:
RULE_VERSION = "v5.1-pyspark"                         # v5.1: greedy 1:1 enforcement, min amount, system blocking
```

---

### CHANGE 2: resolve_matches() Function (Cell 25 — BRD Resolution)

**What changed:** The `resolve_matches()` function was completely rewritten from a single-pass "best fin per axis" to a **stable-marriage iterative algorithm** that enforces exclusive 1:1 matching on both axis_id AND fin_id.

**Find this entire function and replace:**

```python
# OLD (v5.0):
# ── resolve_matches: used ONLY by Greedy layers ─────────────────────────
def resolve_matches(candidates: DataFrame) -> DataFrame:
    """
    For each axis_id, pick the single best fin_id candidate.
    Used by Greedy layers only.
    """
    has_key_strength = "key_strength" in candidates.columns
    ordering = [F.col("priority").asc()]
    if has_key_strength:
        ordering.append(F.col("key_strength").desc())
    ordering.append(F.col("amount_diff").asc_nulls_last())

    w_axis = Window.partitionBy("axis_id").orderBy(*ordering, F.col("fin_id").asc())
    resolved = (
        candidates
        .withColumn("_rn_axis", F.row_number().over(w_axis))
        .filter(F.col("_rn_axis") == 1)
        .drop("_rn_axis")
    )
    return resolved
```

**Replace with:**

```python
# NEW (v5.1):
# ── resolve_matches: Exclusive 1:1 Stable-Marriage (v5.1) ────────────────
def resolve_matches(candidates: DataFrame, max_iter: int = None) -> DataFrame:
    """
    Enforce EXCLUSIVE 1:1 matching between axis_id and fin_id.

    Pandas greedy uses a running set (matched_finstore_indices_greedy) to
    prevent the same Finstore record from being matched by multiple Axis
    trades.  The v5.0 PySpark version only picked 1 best fin per axis but
    did NOT prevent the same fin from serving multiple axes → inflated
    match rate.

    v5.1 fix: two-pass "stable-marriage" iteration:
      Pass A – best fin per axis (window on axis_id, row_number)
      Pass B – best axis per fin (window on fin_id, row_number)
    Repeat until convergence or max_iter.
    """
    if max_iter is None:
        max_iter = GREEDY_MAX_ITER

    has_key_strength = "key_strength" in candidates.columns
    ordering = [F.col("priority").asc()]
    if has_key_strength:
        ordering.append(F.col("key_strength").desc())
    ordering.append(F.col("amount_diff").asc_nulls_last())

    current = candidates
    for i in range(max_iter):
        prev_count = current.count()

        # Pass A: best fin per axis
        w_axis = Window.partitionBy("axis_id").orderBy(*ordering, F.col("fin_id").asc())
        current = (
            current
            .withColumn("_rn_axis", F.row_number().over(w_axis))
            .filter(F.col("_rn_axis") == 1)
            .drop("_rn_axis")
        )

        # Pass B: best axis per fin (prevents fin reuse)
        w_fin = Window.partitionBy("fin_id").orderBy(*ordering, F.col("axis_id").asc())
        current = (
            current
            .withColumn("_rn_fin", F.row_number().over(w_fin))
            .filter(F.col("_rn_fin") == 1)
            .drop("_rn_fin")
        )

        new_count = current.count()
        if new_count == prev_count:
            print(f"    resolve_matches converged in {i+1} iteration(s): {new_count:,} exclusive pairs")
            break
    else:
        print(f"    resolve_matches: reached max_iter={max_iter}, {new_count:,} pairs (may not be fully exclusive)")

    return current
```

---

### CHANGE 3: Unmatched Pools (Cell 30 — Section 11)

**What changed:** Added `MIN_GREEDY_AMOUNT` filter to both axis and finstore unmatched pools. Updated comments and print statements.

**Replace the entire cell with:**

```python
# ============================================================
# COMPUTE UNMATCHED POOLS  (v5.1)
# ============================================================
# ⚡ PERF: No .count() — pools are lazy. They materialise when
#    greedy strategies read them (via their own cache).
#
# brd_matches may have within-rule many-to-many, so we use DISTINCT
# axis_id / fin_id for pool removal — identical to Pandas
# set(matched[idx].unique()).
#
# v5.1: Added MIN_GREEDY_AMOUNT filter to prevent near-zero false positives.

# Distinct Axis / Fin IDs matched in Layer 1
matched_axis_ids = brd_matches.select("axis_id").distinct()
matched_fin_ids  = brd_matches.select("fin_id").distinct()

# Anti-join to get unmatched
axis_unmatched = axis_core.join(matched_axis_ids, on="axis_id", how="left_anti")
fin_unmatched = fin_core.join(matched_fin_ids, on="fin_id", how="left_anti")

# Ensure amounts are clean + enforce minimum amount threshold (v5.1)
axis_unmatched = axis_unmatched.filter(
    F.col(AXIS_AMOUNT_COL).isNotNull()
    & (F.col(AXIS_AMOUNT_COL) != 0)
    & (F.abs(F.col(AXIS_AMOUNT_COL)) >= MIN_GREEDY_AMOUNT)
)
fin_unmatched = fin_unmatched.filter(
    F.col(FIN_AMOUNT_COL).isNotNull()
    & (F.abs(F.col(FIN_AMOUNT_COL)) >= MIN_GREEDY_AMOUNT)
)

# Normalise counterparty for join
axis_unmatched = axis_unmatched.withColumn(
    "cpty_str", F.trim(F.col("CounterpartyId").cast("string"))
)
fin_unmatched = fin_unmatched.withColumn(
    "cpty_str", F.trim(F.col("counterpartyid").cast("string"))
)

# Persist — materialises during greedy Strategy 1
axis_unmatched = axis_unmatched.persist(StorageLevel.MEMORY_AND_DISK)
fin_unmatched = fin_unmatched.persist(StorageLevel.MEMORY_AND_DISK)

print(f"\n{'='*80}")
print("LAYER 2: GREEDY / PROBABILISTIC MATCHING  (v5.1)")
print(f"{'='*80}")
print(f"Min greedy amount: {MIN_GREEDY_AMOUNT:,.0f}")
print(f"Max stable-marriage iterations: {GREEDY_MAX_ITER}")
print("Unmatched pools prepared (will materialise during Strategy 1).")
```

---

### CHANGE 4: Greedy Strategy 1 — Markdown (Cell 31)

**Replace the markdown cell title with:**

```markdown
---
### 🔹 Section 12: Greedy Strategy 1 — Amount + Counterparty + System (1%)

Replaces the Pandas `groupby → iterrows` loop with a **join on counterparty + system + tolerance filter**.
v5.1: Added SourceSystemName equi-join + exclusive 1:1 stable-marriage resolution.
```

---

### CHANGE 5: Greedy Strategy 1 — Code (Cell 32)

**What changed:** Added `SourceSystemName` equi-join to the blocking keys. Added `"None"` filter. Updated description string. Now calls `resolve_matches()` which enforces 1:1.

**Replace the entire cell with:**

```python
# ============================================================
# GREEDY STRATEGY 1: Amount + Counterparty + System (v5.1)
# ============================================================
# Blocked join on counterparty + SourceSystemName → tolerance filter →
# exclusive 1:1 stable-marriage resolution.
#
# v5.1 changes vs v5.0:
#   ✅ Added SourceSystemName equi-join (same-system trades only)
#   ✅ resolve_matches now enforces 1:1 on BOTH axis_id AND fin_id
#   ✅ MIN_GREEDY_AMOUNT already applied in unmatched pools

print(f"\n--- Strategy 1: Amount + Counterparty + System ({GREEDY_TOLERANCE_PCT*100}% tolerance) ---")

# Join on counterparty + system (blocking keys)
greedy1_candidates = (
    axis_unmatched.alias("a")
    .join(
        fin_unmatched.alias("f"),
        on=(
            (F.col("a.cpty_str") == F.col("f.cpty_str"))
            & (F.upper(F.col("a.SourceSystemName")) == F.upper(F.col("f.sourcesystemname")))
        ),
        how="inner"
    )
    # Filter: exclude nan/empty counterparties
    .filter(
        (F.col("a.cpty_str").isNotNull()) &
        (F.col("a.cpty_str") != "") &
        (F.col("a.cpty_str") != "nan") &
        (F.col("a.cpty_str") != "None")
    )
    # Compute difference and tolerance
    .withColumn("amount_diff", F.abs(F.col(f"a.{AXIS_AMOUNT_COL}") - F.col(f"f.{FIN_AMOUNT_COL}")))
    .withColumn("tolerance", F.abs(F.col(f"a.{AXIS_AMOUNT_COL}")) * GREEDY_TOLERANCE_PCT)
    # Filter within tolerance
    .filter(F.col("amount_diff") <= F.col("tolerance"))
    # Select candidate edge columns
    .select(
        F.col("a.axis_id").alias("axis_id"),
        F.col("f.fin_id").alias("fin_id"),
        F.lit(16).cast("int").alias("priority"),
        F.lit("GREEDY").alias("category"),
        F.lit(1).cast("int").alias("brd_priority"),
        F.lit(f"Greedy: Amount+Counterparty+System ({GREEDY_TOLERANCE_PCT*100}%)").alias("description"),
        F.col("amount_diff"),
        (F.col("amount_diff") / F.greatest(F.abs(F.col(f"a.{AXIS_AMOUNT_COL}")), F.lit(1e-9))).alias("amount_rel_diff"),
        F.lit(0).cast("int").alias("key_strength"),
    )
)

# Resolve exclusive 1:1 (stable-marriage: best fin per axis + best axis per fin)
greedy1_matches = resolve_matches(greedy1_candidates)
greedy1_matches = (
    greedy1_matches
    .withColumn("MatchLayer", F.lit("GREEDY"))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("rule_version", F.lit(RULE_VERSION))
    .withColumn("match_timestamp", F.lit(RUN_TIMESTAMP))
)
greedy1_matches = greedy1_matches.persist(StorageLevel.MEMORY_AND_DISK)

# ⚡ This .count() is needed — Strategy 2 uses anti-join on these IDs
greedy1_count = greedy1_matches.count()
print(f"Strategy 1 matches: {greedy1_count:,}")
```

---

### CHANGE 6: Greedy Strategy 2 — Markdown (Cell 33)

**Replace the markdown cell with:**

```markdown
---
### 🔹 Section 13: Greedy Strategy 2 — Amount + System (0.1%)

Uses **amount bucket blocking + SourceSystemName equi-join** to avoid cross-system false positives.
Each Axis record expands to 3 bucket rows (bucket-1, bucket, bucket+1)
then joins Finstore on bucket + system. v5.1: exclusive 1:1 resolution.
```

---

### CHANGE 7: Greedy Strategy 2 — Code (Cell 34)

**What changed:** Added `SourceSystemName` equi-join to the bucket join condition. Updated description string. Now calls `resolve_matches()` which enforces 1:1.

**Replace the entire cell with:**

```python
# ============================================================
# GREEDY STRATEGY 2: Amount + System (0.1% tolerance) with Bucket Blocking (v5.1)
# ============================================================
# v5.1 changes vs v5.0:
#   ✅ Added SourceSystemName equi-join (same-system trades only)
#   ✅ resolve_matches now enforces exclusive 1:1 (stable-marriage)
#   ✅ MIN_GREEDY_AMOUNT already applied in unmatched pools
#   ✅ Pool properly excludes S1-matched axis AND fin

print(f"\n--- Strategy 2: Amount + System ({STRICT_TOLERANCE_PCT*100}% tolerance) ---")

# Remove records already matched in Strategy 1 (both axis AND fin)
greedy1_axis_ids = greedy1_matches.select("axis_id")
greedy1_fin_ids = greedy1_matches.select("fin_id")

axis_remaining_s2 = axis_unmatched.join(greedy1_axis_ids, on="axis_id", how="left_anti")
fin_remaining_s2 = fin_unmatched.join(greedy1_fin_ids, on="fin_id", how="left_anti")

# ⚡ PERF: Removed .count() that was here — it was purely diagnostic
#    and triggered an extra Spark job on the remaining pool.

# Create amount buckets (native Spark — no UDF)
axis_remaining_s2 = axis_remaining_s2.withColumn(
    "amount_bucket",
    (F.floor(F.col(AXIS_AMOUNT_COL) / BUCKET_SIZE) * BUCKET_SIZE).cast("long")
)

fin_remaining_s2 = fin_remaining_s2.withColumn(
    "amount_bucket",
    (F.floor(F.col(FIN_AMOUNT_COL) / BUCKET_SIZE) * BUCKET_SIZE).cast("long")
)

# Expand axis buckets to ±1 for neighbour search
# This creates 3 rows per axis trade — much cheaper than cross-join
bucket_offsets = spark.createDataFrame(
    [(-BUCKET_SIZE,), (0,), (BUCKET_SIZE,)],
    ["_offset"]
)

axis_expanded = (
    axis_remaining_s2
    .crossJoin(F.broadcast(bucket_offsets))
    .withColumn("search_bucket", F.col("amount_bucket") + F.col("_offset"))
    .drop("_offset")
)

# Join on bucket + SourceSystemName (v5.1: same-system only)
greedy2_candidates = (
    axis_expanded.alias("a")
    .join(
        fin_remaining_s2.alias("f"),
        on=(
            (F.col("a.search_bucket") == F.col("f.amount_bucket"))
            & (F.upper(F.col("a.SourceSystemName")) == F.upper(F.col("f.sourcesystemname")))
        ),
        how="inner"
    )
    .withColumn("amount_diff", F.abs(F.col(f"a.{AXIS_AMOUNT_COL}") - F.col(f"f.{FIN_AMOUNT_COL}")))
    .withColumn("tolerance", F.abs(F.col(f"a.{AXIS_AMOUNT_COL}")) * STRICT_TOLERANCE_PCT)
    .filter(F.col("amount_diff") <= F.col("tolerance"))
    .dropDuplicates(["axis_id", "fin_id"])
    .select(
        F.col("a.axis_id").alias("axis_id"),
        F.col("f.fin_id").alias("fin_id"),
        F.lit(17).cast("int").alias("priority"),
        F.lit("GREEDY").alias("category"),
        F.lit(2).cast("int").alias("brd_priority"),
        F.lit(f"Greedy: Amount+System Strict ({STRICT_TOLERANCE_PCT*100}%)").alias("description"),
        F.col("amount_diff"),
        (F.col("amount_diff") / F.greatest(F.abs(F.col(f"a.{AXIS_AMOUNT_COL}")), F.lit(1e-9))).alias("amount_rel_diff"),
        F.lit(0).cast("int").alias("key_strength"),
    )
)

# Resolve exclusive 1:1 (stable-marriage: best fin per axis + best axis per fin)
greedy2_matches = resolve_matches(greedy2_candidates)
greedy2_matches = (
    greedy2_matches
    .withColumn("MatchLayer", F.lit("GREEDY"))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("rule_version", F.lit(RULE_VERSION))
    .withColumn("match_timestamp", F.lit(RUN_TIMESTAMP))
)
greedy2_matches = greedy2_matches.persist(StorageLevel.MEMORY_AND_DISK)

greedy2_count = greedy2_matches.count()
print(f"Strategy 2 matches: {greedy2_count:,}")
```

---

### CHANGE 8: Greedy Layer Summary (Cell 36)

**Replace the entire cell with:**

```python
# ============================================================
# LAYER 2 (GREEDY) SUMMARY  (v5.1)
# ============================================================
# ⚡ No new Spark jobs — uses pre-computed counts.

total_greedy = greedy1_count + greedy2_count

print(f"\n{'='*60}")
print("LAYER 2 (GREEDY) SUMMARY  (v5.1 — exclusive 1:1)")
print(f"{'='*60}")
print(f"Strategy 1 (Amount+Counterparty+System): {greedy1_count:,}")
print(f"Strategy 2 (Amount+System Strict):       {greedy2_count:,}")
print(f"Total Greedy matches:                    {total_greedy:,}")
print(f"Greedy rate:                             {total_greedy/ORIGINAL_AXIS_COUNT*100:.2f}%")
```

---

### CHANGE 9: 🆕 NEW CELL — Greedy Quality Diagnostics (Insert AFTER Cell 36)

**This is an entirely new cell. Insert it between the Greedy Summary and Section 15.**

```python
# ============================================================
# GREEDY MATCH QUALITY DIAGNOSTICS  (v5.1)
# ============================================================
# These checks validate that greedy matches are genuine and not
# false positives from amount coincidence.

print(f"\n{'='*80}")
print("GREEDY MATCH QUALITY DIAGNOSTICS")
print(f"{'='*80}")

# ── 1. Fin_id exclusivity within greedy ──────────────────────────────────
# Each fin_id must appear at most once across greedy S1 + S2 combined
greedy_all = greedy1_matches.select("axis_id", "fin_id", "description").unionByName(
    greedy2_matches.select("axis_id", "fin_id", "description")
)
fin_reuse = (
    greedy_all
    .groupBy("fin_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
)
fin_reuse_count = fin_reuse.count()
if fin_reuse_count > 0:
    print(f"  ⚠️  {fin_reuse_count:,} fin_id(s) matched to MULTIPLE Axis trades in greedy")
    fin_reuse.orderBy(F.col("cnt").desc()).show(10, truncate=False)
else:
    print(f"  ✅ Fin_id exclusivity: every fin_id matched at most once in greedy")

# ── 2. Fin_id exclusivity across BRD + Greedy ───────────────────────────
brd_fin_ids = brd_matches.select("fin_id").distinct()
greedy_fin_ids = greedy_all.select("fin_id").distinct()
cross_layer_fin_overlap = brd_fin_ids.intersect(greedy_fin_ids).count()
if cross_layer_fin_overlap > 0:
    print(f"  ⚠️  {cross_layer_fin_overlap:,} fin_id(s) appear in BOTH BRD and Greedy layers")
else:
    print(f"  ✅ No fin_id overlap between BRD and Greedy layers (pool anti-join working)")

# ── 3. Amount difference distribution ────────────────────────────────────
print(f"\n  Amount Difference Distribution (Greedy Strategy 1):")
if greedy1_count > 0:
    greedy1_matches.select("amount_diff", "amount_rel_diff").summary(
        "min", "25%", "50%", "75%", "90%", "max"
    ).show(truncate=False)
else:
    print("    (no Strategy 1 matches)")

print(f"  Amount Difference Distribution (Greedy Strategy 2):")
if greedy2_count > 0:
    greedy2_matches.select("amount_diff", "amount_rel_diff").summary(
        "min", "25%", "50%", "75%", "90%", "max"
    ).show(truncate=False)
else:
    print("    (no Strategy 2 matches)")

# ── 4. Greedy matches by SourceSystemName ────────────────────────────────
# Shows which systems contribute most greedy matches — useful for
# identifying systems with poor BRD key coverage.
print(f"\n  Greedy Matches by SourceSystemName:")
greedy_with_system = (
    greedy1_matches.select("axis_id", F.lit("S1").alias("strategy"))
    .unionByName(greedy2_matches.select("axis_id", F.lit("S2").alias("strategy")))
    .join(axis_core.select("axis_id", "SourceSystemName"), on="axis_id", how="left")
)
greedy_with_system.groupBy("SourceSystemName", "strategy") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(20, truncate=False)

print(f"{'='*80}")
```

---

### CHANGE 10: Final Consolidation — Comments Only (Cell 39)

**What changed:** Updated the header comment block.

**Find and replace the comment block at the top of the cell:**

```python
# OLD:
# FINAL CONSOLIDATION  —  Waterfall Parity
# ...
# Greedy layers: 1 row per axis (resolve_matches window dedup)

# NEW:
# FINAL CONSOLIDATION  —  Waterfall Parity (v5.1)
# ...
# Greedy layers: exclusive 1:1 (stable-marriage resolve_matches v5.1)
#   Each axis matches at most 1 fin, each fin matched by at most 1 axis.
```

---

### CHANGE 11: Section 15a Markdown (Cell 40)

**Replace with 6 invariants instead of 5:**

```markdown
---
### 🔹 Section 15a: Match Count Verification (6 Invariants)

Six assertions that **must** all pass before saving any results:

1. **Pool isolation**: no `axis_id` appears in both BRD and Greedy layers
2. **Partition**: no `axis_id` appears in both matched and unmatched sets
3. **Axis reconciliation**: `unique_matched_axis + unmatched_axis == ORIGINAL_AXIS_COUNT`
4. **Pair uniqueness**: no duplicate `(axis_id, fin_id)` pairs in all_matches — multi-leg rows are legitimate ONLY as 1-axis → N-distinct-fin
5. **Finstore reconciliation**: `unique_matched_fin + unmatched_fin == ORIGINAL_FINSTORE_COUNT`
6. **Greedy fin_id exclusivity**: each `fin_id` appears at most once across greedy S1+S2 (1:1 enforcement)

> `axis_id` **can** appear multiple times within BRD matches if its best rule
> produced multiple finstore hits (within-rule many-to-many). The checks verify
> **unique axis** counts, not raw row counts.
```

---

### CHANGE 12: Section 15a Code — CHECK 6 Added (Cell 41)

**What changed:** Header updated (v5.0→v5.1, 5→6 invariants). New CHECK 6 block added after CHECK 5. Final summary updated.

**Replace the header:**

```python
# OLD:
# SECTION 15a: MATCH COUNT VERIFICATION  (v5.0)
# Five invariants that MUST hold...
print("MATCH COUNT VERIFICATION  (5 invariants)")

# NEW:
# SECTION 15a: MATCH COUNT VERIFICATION  (v5.1)
# Six invariants that MUST hold...
# CHECK 6: Greedy fin_id exclusivity — each fin_id at most once in greedy
print("MATCH COUNT VERIFICATION  (6 invariants, v5.1)")
```

**Add this block AFTER CHECK 5 (after the `else: print("✅ CHECK 5 PASSED...")` block):**

```python
# ── CHECK 6: Greedy fin_id exclusivity (v5.1) ───────────────────────
# Each fin_id must appear at most once across greedy S1 + S2.
# This verifies the stable-marriage resolve_matches is working.
greedy_combined_fins = (
    greedy1_matches.select("fin_id")
    .unionAll(greedy2_matches.select("fin_id"))
)
greedy_fin_total = greedy_combined_fins.count()
greedy_fin_distinct = greedy_combined_fins.distinct().count()
greedy_fin_dupes = greedy_fin_total - greedy_fin_distinct
if greedy_fin_dupes != 0:
    errors.append(
        f"CHECK 6 FAILED: {greedy_fin_dupes:,} fin_id(s) reused across "
        f"greedy matches ({greedy_fin_total:,} total, {greedy_fin_distinct:,} distinct)"
    )
else:
    print(f"✅ CHECK 6 PASSED: zero fin_id reuse in greedy layer "
          f"({greedy_fin_total:,} total == {greedy_fin_distinct:,} distinct)")
```

**Replace the final success block:**

```python
# OLD:
    print("ALL 5 VERIFICATION CHECKS PASSED ✅")
    # ... (no greedy fin exclusivity line)

# NEW:
    print("ALL 6 VERIFICATION CHECKS PASSED ✅")
    # ... add this line after "Pair uniqueness":
    print(f"  Greedy fin exclusivity: {greedy_fin_total:,} greedy fins, all distinct")
```

---

### CHANGE 13: POC Summary Report Builder (Cells 62–63)

**Markdown cell — change title:**

```markdown
## 🔹 Section 22b: POC Summary Report Builder (v5.1)
```

**Code cell — change header + matching semantics block:**

```python
# OLD:
print(f"  📊 POC SUMMARY REPORT — Sequential Waterfall v5.0")

# NEW:
print(f"  📊 POC SUMMARY REPORT — Sequential Waterfall v5.1")
```

```python
# OLD:
# SECTION 22b: POC SUMMARY REPORT BUILDER (v5.0)

# NEW:
# SECTION 22b: POC SUMMARY REPORT BUILDER (v5.1)
```

**In the report f-string, replace the MATCHING SEMANTICS block:**

```python
# OLD:
MATCHING SEMANTICS  (v5.0 — Sequential Waterfall)
  ...
  Greedy layers: 1 axis → 1 best fin (window dedup)           : Pandas nsmallest
  ...
  Runtime assertions verified in Section 15a

# NEW:
MATCHING SEMANTICS  (v5.1 — Sequential Waterfall + Exclusive Greedy)
  ...
  Greedy layers: exclusive 1:1 stable-marriage resolution      : Pandas set tracking
    - Same fin_id cannot be matched by multiple Axis trades
    - SourceSystemName equi-join prevents cross-system matches
    - MIN_GREEDY_AMOUNT={MIN_GREEDY_AMOUNT} prevents near-zero false positives
  ...
  6 runtime assertions verified in Section 15a
```

---

## Root Causes Fixed

| # | Bug | Impact | Fix |
|---|-----|--------|-----|
| 1 | `resolve_matches()` only enforced 1 fin per axis, NOT 1 axis per fin | Same fin_id matched by 100+ axis trades → massive inflation | Stable-marriage: two-pass iterative (best fin/axis + best axis/fin) |
| 2 | Strategy 1 joined on counterparty only, no system check | Cross-system false positives (Murex ↔ Summit with same counterparty) | Added `SourceSystemName` equi-join |
| 3 | Strategy 2 joined on amount bucket only, no system check | Any trade with similar GBP amount matched regardless of system | Added `SourceSystemName` equi-join |
| 4 | No minimum amount threshold | Near-zero amounts (£0.01) trivially matched other near-zero amounts | `MIN_GREEDY_AMOUNT = 100.0` filter on unmatched pools |

---

## Expected Impact

- **Greedy match rate**: Should drop from ~19% to ~2–5% (genuine matches only)
- **Overall match rate**: May drop from ~99% to ~82–85% (realistic)
- **All 6 invariant checks**: Should pass (including new CHECK 6)
- **No fin_id reuse**: Each Finstore record matched at most once in greedy layer
