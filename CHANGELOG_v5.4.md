# Changelog — v5.4: BRD Checkpoint, Greedy Resilience, Reduced Iterations

**Date:** 2026-03-06  
**Previous version:** v5.3 (commit `0f62d45`)  
**This version:** v5.4 (commit `00b22b8`)  
**Branch:** `main`  
**Diff stats:** 502 insertions, 312 deletions

---

## Problem — v5.3 Operational Fragility

v5.3 introduced the iterative shrinking-pool greedy, but had **no resilience**:

- **No BRD checkpoint**: If greedy crashed (OOM, timeout, cluster preemption), all BRD results were lost — hours of deterministic matching wasted.
- **Hard-fail verification**: A single invariant violation raised `AssertionError`, blocking ALL saves including fully valid BRD results.
- **Lineage explosion**: Iterative anti-joins built unbounded Spark DAG lineage chains, causing Catalyst OOM after ~8 iterations.
- **Memory leaks**: Persisted batch DataFrames inside greedy loops were never unpersisted.
- **Hardcoded iteration caps**: `S1_MAX_ITER=15` and `S2_MAX_ITER=15` were buried inside code cells, not configurable.
- **Dead code**: `resolve_matches()` still appeared active but was no longer called.
- **Empty greedy breaks downstream**: If greedy was skipped or produced 0 matches, diagnostics, system breakdown, and save cells would crash.

---

## Changes Made (13 scenarios)

### 1. Config Cell — Greedy Iteration Parameters

**Before (v5.3):**
```python
GREEDY_MAX_ITER = 5             # Max stable-marriage iterations for 1:1 enforcement
```
No checkpoint or resilience settings. Iteration limits hardcoded inside S1/S2 cells (`S1_MAX_ITER = 15`, `S2_MAX_ITER = 15`).

**After (v5.4):**
```python
S1_MAX_ITER = 8                 # Strategy 1 iteration cap (typically converges in 3-5)
S2_MAX_ITER = 5                 # Strategy 2 iteration cap (broader join → fewer iters to limit cost)
LINEAGE_CHECKPOINT_EVERY = 3    # Truncate Spark DAG lineage every N iterations

# CHECKPOINT & RESILIENCE  (v5.4)
SAVE_BRD_CHECKPOINT = True      # Save brd_matches + unmatched pools to Delta after BRD
RUN_GREEDY = True               # Set False to skip greedy entirely (BRD-only run)
BRD_CHECKPOINT_DIR = f"{INPUT_DIR}/matching_results/_brd_checkpoint"
```

**Why:**
- `GREEDY_MAX_ITER` replaced by separate `S1_MAX_ITER`/`S2_MAX_ITER` with reduced defaults (8/5 vs 15/15).
- `LINEAGE_CHECKPOINT_EVERY` controls lineage truncation frequency.
- `SAVE_BRD_CHECKPOINT` + `RUN_GREEDY` + `BRD_CHECKPOINT_DIR` enable checkpoint/resilience.

---

### 2. Config Cell — Rule Version & Diagnostic Output

**Before (v5.3):**
```python
RULE_VERSION = "v5.3-pyspark"   # v5.3: iterative greedy (shrinking-pool), no system join
```
Config banner only printed greedy tolerances, bucket size, and `GREEDY_MAX_ITER`.

**After (v5.4):**
```python
RULE_VERSION = "v5.4-pyspark"   # v5.4: BRD checkpoint, greedy resilience, reduced iterations
```
Config banner now also prints:
```
  S1 max iterations: 8
  S2 max iterations: 5
  RUN_GREEDY: True
CHECKPOINT:
  SAVE_BRD_CHECKPOINT: True
  BRD_CHECKPOINT_DIR: .../matching_results/_brd_checkpoint
```

---

### 3. `resolve_matches()` Function — Deprecated

**Before (v5.3):**
```python
# ── resolve_matches: Exclusive 1:1 Stable-Marriage (v5.1) ────────────────
def resolve_matches(candidates: DataFrame, max_iter: int = None) -> DataFrame:
```
Appeared to be an active utility function.

**After (v5.4):**
```python
# ── resolve_matches: DEPRECATED (v5.4) ──────────────────────────────────
# This function is no longer called.  Greedy strategies use iterative
# shrinking-pool loops (v5.3+).  BRD uses a window-based best-priority
# filter directly.  Kept for reference only — will be removed in v6.
def resolve_matches(candidates: DataFrame, max_iter: int = None) -> DataFrame:
```

**Why:** The function was dead code since v5.3. Marked DEPRECATED to prevent confusion.

---

### 4. BRD Checkpoint — New Cells (Markdown + Code)

**Before (v5.3):**
No BRD checkpoint mechanism existed. If greedy crashed, BRD results were lost.

**After (v5.4):**

New markdown cell:
```
### 🔹 Section 10c: BRD Checkpoint Save (v5.4)

Save BRD deterministic results **before** greedy starts so they survive
greedy failures (OOM, timeout, cluster preemption).

Saved tables:
- `_brd_checkpoint/brd_matches`
- `_brd_checkpoint/brd_unmatched_axis`
- `_brd_checkpoint/brd_unmatched_fin`
```

New code cell saves three Delta tables + `_metadata.json` (with `run_id`, `batch_id`, `rule_version`, match stats). Uses `dbutils.fs.put` on Databricks, falls back to `os.makedirs` locally. Controlled by `SAVE_BRD_CHECKPOINT` flag.

---

### 5. Unmatched Pools Banner (Greedy Layer 2 Header)

**Before (v5.3):**
```python
print("LAYER 2: GREEDY / PROBABILISTIC MATCHING  (v5.1)")
print(f"Min greedy amount: {MIN_GREEDY_AMOUNT:,.0f}")
print(f"Max stable-marriage iterations: {GREEDY_MAX_ITER}")
```

**After (v5.4):**
```python
print("LAYER 2: GREEDY / PROBABILISTIC MATCHING  (v5.4)")
print(f"RUN_GREEDY: {RUN_GREEDY}")
print(f"Min greedy amount: {MIN_GREEDY_AMOUNT:,.0f}")
print(f"S1 max iterations: {S1_MAX_ITER}  |  S2 max iterations: {S2_MAX_ITER}")
print(f"Lineage checkpoint every: {LINEAGE_CHECKPOINT_EVERY} iterations")
```

**Why:** Shows `RUN_GREEDY` flag status, per-strategy iteration limits, and lineage truncation frequency instead of stale `GREEDY_MAX_ITER` reference.

---

### 6. Greedy Strategy 1 (Amount + Counterparty) — Resilience & Lineage

**Before (v5.3):**
```python
# Flat code at top level (no guard, no try/except)
greedy1_all_batches = []
s1_axis_pool = axis_unmatched
s1_fin_pool  = fin_unmatched
S1_MAX_ITER  = 15  # safety cap — typically converges in 3-8

for s1_iter in range(1, S1_MAX_ITER + 1):
    # ... join, window dedup, batch, shrink pools ...
    batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
    greedy1_all_batches.append(batch)
    # ... anti-join shrink ...

# Combine all batches
if greedy1_all_batches:
    greedy1_matches = greedy1_all_batches[0]
    for b in greedy1_all_batches[1:]:
        greedy1_matches = greedy1_matches.unionByName(b)
    # ... metadata columns ...
else:
    greedy1_matches = spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA)

greedy1_matches = greedy1_matches.persist(StorageLevel.MEMORY_AND_DISK)
greedy1_count = greedy1_matches.count()
```
- No `RUN_GREEDY` guard
- `S1_MAX_ITER = 15` hardcoded inside cell
- No `try/except` — failure crashes entire notebook
- No lineage truncation — Catalyst DAG grows unbounded
- Persisted batches never unpersisted (memory leak)

**After (v5.4):**
```python
_greedy_failed = False  # flag for downstream resilience

if not RUN_GREEDY:
    print("⏭️  GREEDY SKIPPED (RUN_GREEDY=False)")
    greedy1_matches = spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA)
    greedy1_count = 0
else:
    print(f"... [iterative, max {S1_MAX_ITER} iters] ---")

    try:
        greedy1_all_batches = []
        s1_axis_pool = axis_unmatched
        s1_fin_pool  = fin_unmatched

        for s1_iter in range(1, S1_MAX_ITER + 1):
            # ... same join + window dedup + batch logic ...

            # 6. Truncate lineage every N iterations to prevent catalyst explosion
            if s1_iter % LINEAGE_CHECKPOINT_EVERY == 0:
                s1_axis_pool = s1_axis_pool.localCheckpoint(eager=True)
                s1_fin_pool  = s1_fin_pool.localCheckpoint(eager=True)
                print(f"    ↳ lineage truncated (iteration {s1_iter})")

        # Combine all batches
        if greedy1_all_batches:
            greedy1_matches = greedy1_all_batches[0]
            for b in greedy1_all_batches[1:]:
                greedy1_matches = greedy1_matches.unionByName(b)
            # ... metadata columns ...
            # Unpersist individual batches — they're now part of the union
            for b in greedy1_all_batches:
                try:
                    b.unpersist()
                except Exception:
                    pass
        else:
            greedy1_matches = spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA)

        greedy1_matches = greedy1_matches.persist(StorageLevel.MEMORY_AND_DISK)
        greedy1_count = greedy1_matches.count()

    except Exception as e:
        print(f"\n❌ GREEDY S1 FAILED: {e}")
        _greedy_failed = True
        greedy1_matches = spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA)
        greedy1_matches = greedy1_matches.persist(StorageLevel.MEMORY_AND_DISK)
        greedy1_count = 0
```

**Key differences:**
| Aspect | v5.3 | v5.4 |
|---|---|---|
| `RUN_GREEDY` guard | No | Yes — skips with empty DataFrame |
| `S1_MAX_ITER` | `15` hardcoded in cell | `8` from config cell |
| `try/except` | No — failure crashes notebook | Yes — sets `_greedy_failed = True`, creates empty result |
| Lineage truncation | None | `localCheckpoint(eager=True)` every 3 iterations |
| Batch unpersist | Never — memory leak | Loop unpersists all batches after union |
| `_greedy_failed` flag | Didn't exist | Set on failure, used by S2 + downstream |

---

### 7. Greedy Strategy 2 (Amount Bucket) — Resilience & Lineage

**Before (v5.3):**
```python
# Flat code at top level (no guard, no try/except)
S2_MAX_ITER = 15  # safety cap
for s2_iter in range(1, S2_MAX_ITER + 1):
    # ... crossJoin + bucket join + window dedup ...
    batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
    # ... anti-join shrink ...
```
Same issues as S1: no guard, no try/except, no lineage truncation, no batch unpersist.

**After (v5.4):**
```python
if not RUN_GREEDY or _greedy_failed:
    if _greedy_failed:
        print("⏭️  GREEDY S2 SKIPPED (S1 failed)")
    else:
        print("⏭️  GREEDY S2 SKIPPED (RUN_GREEDY=False)")
    greedy2_matches = spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA)
    greedy2_matches = greedy2_matches.persist(StorageLevel.MEMORY_AND_DISK)
    greedy2_count = 0
else:
    try:
        # ... same bucket join logic, now indented under try ...
        # 7. Truncate lineage every N iterations
        if s2_iter % LINEAGE_CHECKPOINT_EVERY == 0:
            s2_axis_pool = s2_axis_pool.localCheckpoint(eager=True)
            s2_fin_pool  = s2_fin_pool.localCheckpoint(eager=True)

        # ... combine batches ...
        # Unpersist individual batches
        for b in greedy2_all_batches:
            try:
                b.unpersist()
            except Exception:
                pass

    except Exception as e:
        print(f"\n❌ GREEDY S2 FAILED: {e}")
        _greedy_failed = True
        greedy2_matches = spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA)
        greedy2_matches = greedy2_matches.persist(StorageLevel.MEMORY_AND_DISK)
        greedy2_count = 0
```

**Key differences:**
| Aspect | v5.3 | v5.4 |
|---|---|---|
| `RUN_GREEDY` / `_greedy_failed` guard | No | Yes — skips if greedy disabled or S1 failed |
| `S2_MAX_ITER` | `15` hardcoded in cell | `5` from config cell |
| `try/except` | No | Yes — sets `_greedy_failed`, creates empty result |
| Lineage truncation | None | `localCheckpoint(eager=True)` every 3 iterations |
| Batch unpersist | Never | Loop unpersists all batches after union |

---

### 8. Greedy Summary Cell

**Before (v5.3):**
```python
print("LAYER 2 (GREEDY) SUMMARY  (v5.3 — iterative shrinking-pool)")
# ... print counts directly ...
```
No status-aware messaging.

**After (v5.4):**
```python
print("LAYER 2 (GREEDY) SUMMARY  (v5.4 — iterative shrinking-pool)")
if not RUN_GREEDY:
    print("  ⏭️  Greedy was SKIPPED (RUN_GREEDY=False)")
elif _greedy_failed:
    print("  ⚠️  Greedy PARTIALLY FAILED — partial results only")
# ... print counts ...
```

**Why:** Users now see immediately whether greedy was skipped, failed, or completed normally.

---

### 9. Greedy Match Quality Diagnostics

**Before (v5.3):**
```python
# GREEDY MATCH QUALITY DIAGNOSTICS  (v5.1)
# Ran unconditionally — crashed if greedy was empty/skipped

# ── 1. Fin_id exclusivity within greedy ─────
greedy_all = greedy1_matches.select(...).unionByName(...)
# ... 4 diagnostic sections run regardless ...
```

**After (v5.4):**
```python
# GREEDY MATCH QUALITY DIAGNOSTICS  (v5.4)
# v5.4: Guards all sections with total_greedy > 0 check.

if total_greedy == 0:
    print("  ⏭️  No greedy matches to diagnose (greedy skipped, failed, or found no matches).")
else:
    # ── 1. Fin_id exclusivity within greedy ─────
    greedy_all = greedy1_matches.select(...).unionByName(...)
    # ... all 4 diagnostic sections inside `else` block ...
```

**Why:** Prevents crashes when greedy produced 0 matches (skipped, failed, or legitimately empty).

---

### 10. Final Consolidation — Version Comments

**Before (v5.3):**
```python
# FINAL CONSOLIDATION  —  Waterfall Parity (v5.1)
# Greedy layers: exclusive 1:1 (stable-marriage resolve_matches v5.1)
```

**After (v5.4):**
```python
# FINAL CONSOLIDATION  —  v5.4
# Greedy layers: exclusive 1:1 (iterative shrinking-pool v5.3+)
```

**Why:** Removed stale "stable-marriage" / "resolve_matches v5.1" references. No logic change.

---

### 11. Match Count Verification — Soft-Fail

**Before (v5.3):**
```python
# SECTION 15a: MATCH COUNT VERIFICATION  (v5.1)
# Six invariants that MUST hold before any result is trusted.
# If any fails, the notebook raises AssertionError and stops.

print("MATCH COUNT VERIFICATION  (6 invariants, v5.1)")
# ... run 6 checks ...

# ── Fail loudly if any check failed ─────
if errors:
    for e in errors:
        print(f"  ❌ {e}")
    raise AssertionError(
        f"{len(errors)} verification check(s) failed — "
        f"do NOT trust downstream reports until resolved."
    )
```
**Behaviour:** Any verification failure killed the notebook — no saves, all BRD+greedy work lost.

**After (v5.4):**
```python
# SECTION 15a: MATCH COUNT VERIFICATION  (v5.4)
# Six invariants validated before saving.
# v5.4: SOFT-FAIL — logs errors and sets _verification_passed = False
#       but does NOT raise AssertionError.  Results are still saved
#       (downstream consumers check the flag / verification_status column).

print("MATCH COUNT VERIFICATION  (6 invariants, v5.4 — soft-fail)")
# ... run same 6 checks ...

# ── SOFT-FAIL: log errors but DO NOT raise ─────
_verification_passed = len(errors) == 0

if errors:
    for e in errors:
        print(f"  ❌ {e}")
    print(f"\n⚠️  {len(errors)} verification check(s) failed.")
    print("   Results will STILL be saved (with verification_status='FAILED').")
    print("   Review failures before using downstream reports.")
    if _greedy_failed:
        print("   ℹ️  Greedy failure may explain some invariant violations.")
else:
    print("ALL 6 VERIFICATION CHECKS PASSED ✅")
```

**Key differences:**
| Aspect | v5.3 | v5.4 |
|---|---|---|
| Failure mode | `raise AssertionError` — notebook halts | Sets `_verification_passed = False` — continues |
| Results saved? | ❌ No saves if any check fails | ✅ Always saves, tags with status column |
| `_verification_passed` flag | Didn't exist | New — used by save cell and reports |
| Greedy-aware message | No | Yes — notes greedy failure may cause invariant violations |
| Stale comments removed | `# Axis_in_scope = ...`, `# Duplicates in output are ONLY legitimate as...`, `# ORIGINAL_FINSTORE_COUNT = ...`, `# This verifies the stable-marriage resolve_matches is working.` | Cleaned up — comments are concise |
| CHECK 6 header | `# Greedy fin_id exclusivity (v5.1)` + `# This verifies the stable-marriage resolve_matches is working.` | `# Greedy fin_id exclusivity` (no stale references) |

---

### 12. Save Base Matches Cell — Status Columns & Empty Greedy Guard

**Before (v5.3):**
```python
# SAVE BASE MATCHES — narrow schema, before enrichment
# This is the lightweight output (~15 cols) for quick downstream queries.
# The enriched wide table (100+ cols) is saved separately in Section 19.

print(f"Saving base (narrow) match results to: {OUTPUT_DIR}")
# --- All matches (BRD + Greedy) — narrow ---
all_matches.write.format("delta").mode("overwrite").save(f"{OUTPUT_DIR}/matched_all_base")
print(f"✅ Saved Delta: matched_all_base ({total_match_rows:,} rows, {len(all_matches.columns)} cols)")

# --- Greedy only (narrow) ---
# ⚡ CORRECTNESS: reuse all_matches (already deduplication-guarded)...
greedy_all_df = all_matches.filter(F.col("MatchLayer") == "GREEDY")
greedy_all_df.write.format("delta").mode("overwrite").save(f"{OUTPUT_DIR}/matched_greedy_layer")
print(f"✅ Saved Delta: matched_greedy_layer ({total_greedy:,} rows)")
```
No status columns. Greedy save ran unconditionally (crashed if empty).

**After (v5.4):**
```python
# SAVE BASE MATCHES — narrow schema, before enrichment (v5.4)
# v5.4: Adds verification_status + greedy_status columns so
#        consumers know if results are fully verified.
#        Saves proceed even when verification or greedy failed.

_v_status = "PASSED" if _verification_passed else "FAILED"
_g_status = "SKIPPED" if not RUN_GREEDY else ("FAILED" if _greedy_failed else "OK")

print(f"  verification_status: {_v_status}")
print(f"  greedy_status:       {_g_status}")

# Tag all_matches with status columns before save
_all_matches_save = (
    all_matches
    .withColumn("verification_status", F.lit(_v_status))
    .withColumn("greedy_status", F.lit(_g_status))
)

# --- All matches (BRD + Greedy) — narrow ---
_all_matches_save.write.format("delta").mode("overwrite").save(f"{OUTPUT_DIR}/matched_all_base")

# --- Greedy only (narrow) ---
if total_greedy > 0:
    greedy_all_df = all_matches.filter(F.col("MatchLayer") == "GREEDY")
    greedy_all_df.write.format("delta").mode("overwrite").save(...)
else:
    # Save empty greedy layer with correct schema
    spark.createDataFrame([], schema=_GREEDY_EMPTY_SCHEMA) \
        .write.format("delta").mode("overwrite").save(...)
    print(f"✅ Saved Delta: matched_greedy_layer (0 rows — greedy ...)")
```

**Key differences:**
| Aspect | v5.3 | v5.4 |
|---|---|---|
| `verification_status` column | Not present | `"PASSED"` or `"FAILED"` on every row |
| `greedy_status` column | Not present | `"OK"`, `"FAILED"`, or `"SKIPPED"` on every row |
| Greedy layer save | Unconditional filter+save (crash if empty) | Guarded: saves real data if `total_greedy > 0`, saves empty schema otherwise |
| Status printed | No | Prints both statuses before saving |

---

### 13. System Breakdown by SourceSystemName — Empty Greedy Guard

**Before (v5.3):**
```python
# Greedy matches by system (unconditional):
print("Greedy Matches by system:")
greedy_all = greedy1_matches.unionByName(greedy2_matches)
greedy_by_system = (
    greedy_all
    .join(axis_core.select("axis_id", "SourceSystemName"), on="axis_id", how="left")
    .groupBy("SourceSystemName")
    .count()
    .orderBy(F.desc("count"))
)
greedy_by_system.show(20, truncate=False)
```
Ran unconditionally — crashed or showed confusing empty tables when greedy was empty.

**After (v5.4):**
```python
if total_greedy > 0:
    print("Greedy Matches by system:")
    greedy_all = greedy1_matches.unionByName(greedy2_matches)
    greedy_by_system = (
        greedy_all
        .join(axis_core.select("axis_id", "SourceSystemName"), on="axis_id", how="left")
        .groupBy("SourceSystemName")
        .count()
        .orderBy(F.desc("count"))
    )
    greedy_by_system.show(20, truncate=False)
else:
    print("Greedy Matches by system: (no greedy matches)")
```

---

### 14. Summary Report (Cell 59)

**Before (v5.3):**
```
PySpark / Databricks Production — Sequential Waterfall v5.0

v5.0 ANTI-EXPLOSION GUARDRAILS
- P15 tightened: ETD-only system filter, SourceSystemName equi-join, top_k=5
```
No greedy/verification status in header. Stale v5.0 references.

**After (v5.4):**
```
PySpark / Databricks Production — v5.4

Greedy:    {_g_status}
Verified:  {_v_status}

v5.4 GUARDRAILS & RESILIENCE
- BRD checkpoint saved before greedy (SAVE_BRD_CHECKPOINT=...)
- Greedy wrapped in try/except — BRD results survive greedy failure
- Verification soft-fail — results saved even if checks fail
- Lineage truncation every {LINEAGE_CHECKPOINT_EVERY} iterations in greedy loops

PERFORMANCE OPTIMISATIONS
- Iterative shrinking-pool greedy (S1 max {S1_MAX_ITER}, S2 max {S2_MAX_ITER} iters)
```

---

### 15. Accuracy Report (Cell 63)

**Before (v5.3):**
```
MATCHING SEMANTICS  (v5.1 — Sequential Waterfall + Exclusive Greedy)
  Greedy layers: exclusive 1:1 stable-marriage resolution      : Pandas set tracking
    - SourceSystemName equi-join prevents cross-system matches
  6 runtime assertions verified in Section 15a
  P15 tightened: ETD-only, SourceSystemName equi-join, top_k=5
```

**After (v5.4):**
```
MATCHING SEMANTICS  (v5.4 — Sequential Waterfall + Iterative Greedy)
  Greedy layers: iterative shrinking-pool (v5.3+)              : Pandas set tracking
    - No SourceSystemName equi-join (removed v5.2 — Finstore has no system col)
    - Lineage truncation every {LINEAGE_CHECKPOINT_EVERY} iterations (v5.4)
  6 runtime assertions verified in Section 15a (soft-fail v5.4)
  BRD checkpoint saved before greedy (SAVE_BRD_CHECKPOINT=...)
  Greedy status: {_g_status}  |  Verification: {_v_status}
```

**Key differences:**
| Aspect | v5.3 | v5.4 |
|---|---|---|
| Greedy description | "stable-marriage resolution" | "iterative shrinking-pool (v5.3+)" |
| SourceSystemName | "equi-join prevents cross-system matches" | "No SourceSystemName equi-join (removed v5.2)" |
| Assertions | "6 runtime assertions verified" | "6 runtime assertions verified (soft-fail v5.4)" |
| P15 reference | "P15 tightened: ETD-only, SourceSystemName equi-join, top_k=5" | Removed (stale) |
| Status reporting | None | Shows `_g_status` + `_v_status` |
| Lineage info | None | Shows `LINEAGE_CHECKPOINT_EVERY` |
| Checkpoint info | None | Shows `SAVE_BRD_CHECKPOINT` status |

---

## Comparison: v5.3 → v5.4

| Aspect | v5.3 | v5.4 |
|---|---|---|
| BRD checkpoint | ❌ None — BRD lost if greedy crashes | ✅ Delta checkpoint + metadata.json |
| Greedy skip flag | ❌ No way to skip greedy | ✅ `RUN_GREEDY=False` for BRD-only |
| Greedy failure handling | ❌ Crash kills notebook | ✅ `try/except` → `_greedy_failed` flag |
| Verification failure | ❌ `raise AssertionError` → no saves | ✅ Soft-fail → saves with `verification_status` column |
| S1 iteration cap | `15` (hardcoded in cell) | `8` (config cell) |
| S2 iteration cap | `15` (hardcoded in cell) | `5` (config cell) |
| Lineage truncation | ❌ None → Catalyst OOM | ✅ `localCheckpoint` every 3 iterations |
| Batch memory leaks | ❌ Persisted batches never freed | ✅ Unpersisted after union |
| Status columns in output | ❌ None | ✅ `verification_status` + `greedy_status` |
| Empty greedy protection | ❌ Diagnostics/save crash | ✅ `if total_greedy > 0:` guards |
| `resolve_matches()` | Appeared active | Marked DEPRECATED |
| Report semantics | Stale v5.0/v5.1 references | Updated to v5.4 |
| Report text | "stable-marriage", "SourceSystemName equi-join" | "iterative shrinking-pool", "No SourceSystemName equi-join" |

---

## What Did NOT Change (from v5.3)

- **All 15 BRD rule definitions** — unchanged
- **`build_candidates_for_rule()`** — unchanged
- **`safe_rule_join()`** — unchanged
- **BRD Resolution (best priority per axis)** — unchanged
- **Pool removal with `.distinct()`** — unchanged
- **Consolidation logic** — unchanged (only comments updated)
- **Greedy matching algorithm** — same iterative shrinking-pool from v5.3
- **Greedy tolerance thresholds** — 1% S1, 0.1% S2 (unchanged)
- **Enrichment cells** — unchanged
- **Unmatched enrichment** — unchanged

---

## Migration

If reverting to v5.3 (no checkpoint, hard-fail verification):
1. Remove `SAVE_BRD_CHECKPOINT`, `RUN_GREEDY`, `BRD_CHECKPOINT_DIR` from config
2. Replace `S1_MAX_ITER`/`S2_MAX_ITER` with inline `15`
3. Remove `LINEAGE_CHECKPOINT_EVERY`
4. Delete BRD checkpoint cells (markdown + code)
5. Un-indent greedy S1/S2 code out of `try/except` and `if RUN_GREEDY` guards
6. Remove `localCheckpoint` calls and batch unpersist loops
7. Restore `raise AssertionError` in verification cell
8. Remove `_verification_passed`, `_greedy_failed`, `_v_status`, `_g_status` variables
9. Remove `verification_status` / `greedy_status` columns from save cell
10. Remove `if total_greedy > 0:` guards from diagnostics, system breakdown, and save cells
