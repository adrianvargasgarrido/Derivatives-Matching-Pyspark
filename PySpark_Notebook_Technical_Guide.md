# Derivatives Matching Engine — PySpark Notebook Technical Guide

**Version:** v4.0-pyspark  
**Date:** 2026-02-23  
**Notebook:** `Derivatives_Matching_PySpark.ipynb`  
**Reference:** BRD Derivatives Matching Specification, `best_practices.md`, `matching_rules.md`

---

## Table of Contents

1. [Overview & Architecture](#1-overview--architecture)
2. [Why Not Just "Port the Pandas Notebook"?](#2-why-not-just-port-the-pandas-notebook)
3. [Section-by-Section Walkthrough](#3-section-by-section-walkthrough)
   - [Section 1 — Spark Session & Configuration](#section-1--spark-session--configuration)
   - [Section 2 — BRD Constants & System Classifications](#section-2--brd-constants--system-classifications)
   - [Section 3 — Matching Rule Definitions](#section-3--matching-rule-definitions)
   - [Section 4 — Load Data (Bronze Layer)](#section-4--load-data-bronze-layer)
   - [Section 5 — Scope Exclusion](#section-5--scope-exclusion)
   - [Section 6 — Silver Layer Derivations](#section-6--silver-layer-derivations)
   - [Section 7 — Core / Wide Split](#section-7--core--wide-split)
   - [Section 8 — Candidate Generation Function](#section-8--candidate-generation-function)
   - [Section 9 — Generate All BRD Candidates](#section-9--generate-all-brd-candidates)
   - [Section 10 — 1-to-1 Resolution via Window Ranking](#section-10--1-to-1-resolution-via-window-ranking)
   - [Section 11 — Compute Unmatched Pools for Greedy](#section-11--compute-unmatched-pools-for-greedy)
   - [Section 12 — Greedy Strategy 1: Amount + Counterparty](#section-12--greedy-strategy-1-amount--counterparty-1)
   - [Section 13 — Greedy Strategy 2: Amount Only (Bucket Blocking)](#section-13--greedy-strategy-2-amount-only-01)
   - [Section 14 — Greedy Layer Summary](#section-14--greedy-layer-summary)
   - [Section 15 — Final Consolidation](#section-15--final-consolidation)
   - [Section 16 — Enrichment: Join Back Wide Columns](#section-16--enrichment-join-back-wide-columns)
   - [Section 17 — Matches by System Breakdown](#section-17--matches-by-system-breakdown)
   - [Section 18 — Remaining Unmatched by System](#section-18--remaining-unmatched-by-system)
   - [Section 18b — Data Quality Validation](#section-18b--data-quality-validation)
   - [Section 18c — Explainability: Unmatched Reason Breakdown](#section-18c--explainability-unmatched-reason-breakdown)
   - [Section 19 — Save Results](#section-19--save-results)
   - [Section 20 — Summary Report](#section-20--summary-report)
   - [Section 21 — Cleanup](#section-21--cleanup)
4. [All 15 BRD Rules — Detailed Reference](#4-all-15-brd-rules--detailed-reference)
   - [SOPHIS Rules (P1–P3)](#sophis-rules-p1p3)
   - [OTC Rules (P4–P13)](#otc-rules-p4p13)
   - [ETD Rules (P14–P15)](#etd-rules-p14p15)
5. [Greedy Strategies — Detailed Reference](#5-greedy-strategies--detailed-reference)
6. [End-to-End Data Flow Diagram](#6-end-to-end-data-flow-diagram)
7. [Key Efficiency Decisions — Summary Table](#7-key-efficiency-decisions--summary-table)

---

## 1. Overview & Architecture

The notebook implements a **two-layer derivatives trade matching engine** that reconciles positions between two systems:

- **Axis** — the internal trade booking system (~4 million rows, 100+ columns)
- **Finstore** — the regulatory capital reporting system (~20 million rows, 100+ columns)

### Two Layers

| Layer | Mechanism | Priority |
|---|---|---|
| **Layer 1 — BRD Deterministic** | 15 rules from the Business Requirements Document; equi-join on trade identifiers | Highest |
| **Layer 2 — Greedy / Probabilistic** | Amount similarity with optional counterparty blocking when no deterministic key is available | Fallback |

### Core Architectural Principle: Candidates → Score → Resolve

Rather than executing rules one-by-one and physically removing matched records from a pool after each rule (the "waterfall mutation" pattern), the notebook uses a **graph-edge approach**:

```
All 15 BRD rules
      │
      ▼
  ┌──────────────────────────────┐
  │  Candidate edges table       │
  │  (axis_id, fin_id, priority, │
  │   amount_diff, key_strength) │
  └──────────────────────────────┘
      │
      ▼  Window ranking (priority ASC, amount_diff ASC)
  ┌──────────────────────────────┐
  │  gold.matches (1-to-1)       │
  └──────────────────────────────┘
      │
      ▼  Anti-join
  ┌──────────────────────────────┐
  │  Unmatched → Greedy Layer    │
  └──────────────────────────────┘
```

This single-pass approach produces **identical results** to the iterative Pandas waterfall while eliminating the 13+ repeated shuffle-and-anti-join cycles that would make direct porting non-viable at scale.

---

## 2. Why Not Just "Port the Pandas Notebook"?

The Pandas notebook uses a classic **iterative pool-removal waterfall**:

```python
for rule in RULES:
    matched = merge(axis_pool, finstore_pool, on=rule.keys)
    axis_pool    = axis_pool[~axis_pool.index.isin(matched_ids)]     # remove matched
    finstore_pool = finstore_pool[~finstore_pool.index.isin(matched_ids)]
```

This works for hundreds of thousands of rows. At 4M × 20M it breaks down in four distinct ways:

### Problem 1 — Repeated wide shuffles

Each of the 15 rules triggers a full equi-join. In Spark, an equi-join on two large DataFrames causes a **shuffle** (data redistribution across nodes). Doing this 15 times sequentially means 15 independent shuffle stages, each potentially moving billions of bytes across the network. That typically translates to hours or days of cluster time.

> **Fix:** Generate all 15 sets of candidate edges in a single loop, union them, and resolve once. The shuffle still happens, but only **once per rule for the narrow candidate schema** (~5 columns) instead of 15 times for the full wide schema.

### Problem 2 — 13 anti-joins to mutate pools

After each rule, the Pandas code removes matched rows from both pools. In Spark this is a `left_anti` join — which is itself a shuffle. Doing 13 anti-joins sequentially compounds the shuffle cost dramatically.

> **Fix:** Pool mutation is replaced by window-ranking. The window function selects the best (lowest-priority) candidate per `axis_id` and per `fin_id` in a single pass. No pools are physically modified between rules.

### Problem 3 — 100+ column shuffle payload

If you join the full wide DataFrames (100+ columns each) inside each rule loop, every shuffle moves extremely wide rows. A 200-column row is ~10× wider than a 20-column row, meaning the same join produces ~10× more network I/O.

> **Fix:** The **Core/Wide Split** (Section 7) ensures all matching is done on ~10–15 column "core" schemas. The full 100+ columns are only joined back once, at the very end (Section 16), on the final matched set which is much smaller.

### Problem 4 — Skew and many-to-many explosions

Keys like `DerivedSophisId` or `fissnumber` may not be globally unique. A single popular key value can match thousands of rows on each side, causing a Cartesian explosion within that partition. One task handles millions of rows while others are idle — the classic **data skew** problem.

> **Fix:** Adaptive Query Execution (AQE) with skew-join handling is explicitly enabled. Candidate generation filters out invalid keys (empty, `nan`, `$`-containing) before joining. The bucket-blocking strategy (Section 13) prevents full cross-joins in the greedy layer.

---

## 3. Section-by-Section Walkthrough

---

### Section 1 — Spark Session & Configuration

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**What it does:** Configures the SparkSession with three critical flags from Spark's **Adaptive Query Execution (AQE)** framework.

**Why it's efficient:**

| Flag | Effect |
|---|---|
| `adaptive.enabled` | AQE rewrites the physical plan at runtime using actual partition statistics. It can change join strategies (e.g., switch a sort-merge join to a broadcast join) after seeing real data sizes. |
| `coalescePartitions.enabled` | After a shuffle, AQE merges small output partitions automatically. This avoids the classic "too many tiny tasks" slowdown that plagues uniformly-partitioned jobs. |
| `skewJoin.enabled` | When one partition is much larger than others (hot key), AQE splits it and replicates the smaller side. This prevents one task from running for hours while the rest finish in seconds. |

**On Databricks** these are enabled by default, but making them explicit ensures they survive any cluster configuration overrides and makes the intent visible to future maintainers.

---

### Section 2 — BRD Constants & System Classifications

```python
OTC_SYSTEMS  = ["IFLOW-EUROPE", "SUMMIT-LONDON", ...]
ETD_SYSTEMS  = ["ODH-GMI-LONDON", "ODH-GMI-NEWYORK", ...]
SOPHIS_SYSTEMS = ["SOPHIS-LONDON", "SOPHIS-NEWYORK", ...]
DELTA1_SYSTEMS = ["DELTA1-LONDON", "DELTA1-NEWYORK"]

AXIS_AMOUNT_COL = "SACCRMTM"
FIN_AMOUNT_COL  = "gbpequivalentamount"
```

**What it does:** Defines all system classification lists and the two amount column names that are used throughout every rule.

**Why it's structured this way:** Centralising constants avoids hard-coded strings scattered across 15 rules. When a system is reclassified (common in real programmes), only this cell needs to change. The lists are Python `list` objects (not sets) because Spark's `isin()` filter accepts lists directly without conversion.

**Lineage & auditability config** is also set here:

```python
RUN_ID        = str(uuid.uuid4())   # unique per notebook execution
BATCH_ID      = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_TIMESTAMP = datetime.now().isoformat()
RULE_VERSION  = "v4.0-pyspark"
```

Every matched row carries these four values. This means any downstream audit can trace any single match back to the exact notebook run, timestamp, and rule version that produced it — a requirement for regulatory capital reporting.

---

### Section 3 — Matching Rule Definitions

```python
MATCHING_RULES = [
    dict(priority=1, category="SOPHIS", ...,
         axis_keys=["DerivedSophisId"], fin_keys=["fissnumber"],
         filter_sophis_only=True, requires_derived_masterbook=False),
    ...
]
```

**What it does:** Defines all 15 BRD rules as a list of plain Python dictionaries.

**Why dictionaries instead of dataclasses:** Spark jobs iterate over these dicts inside a Python driver loop — no Spark operation touches the rule definitions themselves. Dictionaries are slightly more concise for this purpose and avoid importing extra modules on the driver. The schema is consistent enough across all 15 rules that dataclasses add no safety benefit here.

**Key fields explained:**

| Field | Purpose |
|---|---|
| `priority` | Global waterfall order; lower number = higher priority. This is the ordering key in the window-ranking resolution step. |
| `axis_keys` / `fin_keys` | Column name(s) to equi-join on. Multiple keys create composite join conditions without needing to pre-concatenate strings. |
| `filter_sophis_only` | When `True`, the Axis DataFrame is pre-filtered to SOPHIS systems before the join. This drastically reduces the number of rows participating in SOPHIS-specific rules. |
| `requires_derived_masterbook` | When `True`, the rule is skipped entirely if `DerivedMasterbookId` is empty (SDS mapping not available). This prevents a silent no-match rather than throwing an error. |

---

### Section 4 — Load Data (Bronze Layer)

```python
df_axis_full = spark.read.option("header","true").option("inferSchema","true").csv(...)
df_axis_full = df_axis_full
    .withColumn("_ingest_timestamp", F.lit(RUN_TIMESTAMP))
    .withColumn("_source_file",      F.lit(INPUT_FILE_AXIS))
    .withColumn("_batch_id",         F.lit(BATCH_ID))
```

**What it does:** Reads raw CSV files (or Delta tables in production) and immediately stamps each row with three metadata columns.

**Why Bronze metadata matters:**  
In a production Delta Lake architecture the Bronze layer is **append-only** — you never overwrite raw data. The three metadata columns serve different purposes:

- `_ingest_timestamp` — when the row was ingested; used for debugging data freshness issues
- `_source_file` — which file the row came from; critical when inputs arrive from multiple source feeds
- `_batch_id` — ties every row in this pipeline run together; used to reprocess or roll back a batch

**Why `inferSchema`:** For a POC/test dataset this is fine. In production, replacing `inferSchema` with an explicit `StructType` schema is preferred because: (a) it avoids a full CSV scan just to infer types, (b) it fails loudly when the source has unexpected columns, rather than silently casting them.

**SDS mapping** is loaded with a `try/except` because it is optional — rules that require it (`requires_derived_masterbook=True`) are gracefully skipped if the file is absent.

---

### Section 5 — Scope Exclusion

```python
df_axis = df_axis_full.filter(~F.col("SourceSystemName").isin(OUT_OF_SCOPE_SYSTEMS))
```

**What it does:** Removes SOPHIS and DELTA1 system trades from the working Axis DataFrame before any matching begins.

**Why these systems are excluded:** Their trade identifiers (`SourceSystemTradeId`) do not exist in Finstore. This is a **data mapping gap** at source system level, not a matching algorithm failure. Including them would produce a large volume of guaranteed unmatched trades, inflating the unmatched count and obscuring genuine reconciliation breaks.

**Why apply this as a Spark filter (not Pandas masking):** A Spark `filter` is a narrow transformation — it adds a predicate to the query plan and does not trigger a shuffle. When the CSV is read, Spark evaluates the filter partition-by-partition, never materialising the excluded rows into memory.

---

### Section 6 — Silver Layer Derivations

**What it does:** Computes four derived columns on the Axis DataFrame using **native Spark SQL functions only** — no Python UDFs.

#### DerivedSophisId

```python
df_axis = df_axis.withColumn(
    "DerivedSophisId",
    F.when(
        F.col("SourceSystemName").isin(SOPHIS_SYSTEMS) &
        (F.size(F.split(F.col("SourceSystemTradeId"), "-")) >= 3),
        F.element_at(F.split(F.col("SourceSystemTradeId"), "-"), 3)
    ).otherwise(F.lit(""))
)
```

SOPHIS trade IDs follow the format `PREFIX-SYSTEM-{sophisId}`. The third hyphen-delimited segment is the canonical identifier that appears in Finstore's `fissnumber` and `tradeid` fields. `F.split()` + `F.element_at()` perform this extraction natively inside the JVM — no Python function call overhead per row.

#### DerivedDelta1Id

Identical logic applied to `DELTA1_SYSTEMS`. The third segment of DELTA1 trade IDs maps to Finstore's `tradeid`.

#### ReconSubProduct

```python
F.when(F.col("SourceSystemName").isin(ETD_SYSTEMS), "ETD")
 .when(F.col("SourceSystemName").isin(OTC_SYSTEMS),  "OTC")
 .otherwise("OTC-Default")
```

A classification derived entirely from `SourceSystemName`. Used later to restrict ETD matching rules to the correct record subset.

#### Why no Python UDFs?

Python UDFs execute row-by-row in a Python process, crossing the JVM↔Python serialisation boundary for every single row. For 4 million rows this serialisation overhead dwarfs the actual computation. Native Spark SQL functions (`F.split`, `F.element_at`, `F.when`, `F.floor`, etc.) execute inside the JVM on the entire partition at once — typically 10–100× faster.

---

### Section 7 — Core / Wide Split

```python
AXIS_CORE_COLS = ["axis_id", "SourceSystemName", "SourceSystemTradeId",
                   "BookId", "CounterpartyId", "SACCRMTM", ...]
axis_core = df_axis.select(AXIS_CORE_COLS)
axis_wide = df_axis   # retained in full for final enrichment
```

**What it does:** Splits each DataFrame into a **core** (matching schema, ~10–15 columns) and **wide** (full schema, 100+ columns). All matching is done exclusively on the core tables.

**Why this is the single biggest performance win:**

A Spark shuffle moves data proportional to the **size of each row × number of rows**. If each Axis row is 5 KB wide (100 columns of strings and doubles), a join involving both full tables shuffles 4M × 5 KB = **20 GB** of data. If the core schema is 15 columns (~300 bytes per row), the same join shuffles **1.2 GB** — a **17× reduction** in network I/O, disk spill, and memory pressure, for identical matching logic.

The wide tables are never touched during matching. They are joined back exactly once, at the end (Section 16), on the already-small matched set.

**Core tables are cached** because they are reused across all 15 BRD rules. Without caching, Spark would re-read and re-derive them from scratch for each rule.

---

### Section 8 — Candidate Generation Function

```python
def build_candidates_for_rule(axis_df, fin_df, rule):
    # 1. Apply system filter
    # 2. Skip if DerivedMasterbookId required but empty
    # 3. Project to narrow schema (axis_id + amount + join keys only)
    # 4. Filter invalid keys (empty, 'nan', '$')
    # 5. Rename keys to common names (_jk0, _jk1, ...)
    # 6. Equi-join
    # 7. Compute amount_diff, amount_rel_diff, key_strength
    # 8. Return standardised (axis_id, fin_id, priority, ...) schema
```

**What it does:** A single reusable function that generates candidate match pairs for any of the 15 BRD rules.

**Step-by-step reasoning:**

**Step 3 — Project to narrow schema:**  
Before the join, each side is projected to only the columns needed for that specific rule: `axis_id`, the amount column, and the join key(s). This ensures the join shuffle carries the minimum possible payload.

**Step 4 — Filter invalid keys:**  
Keys that are `null`, empty string, `"nan"`, or contain `"$"` are excluded. These represent missing or corrupted identifiers. Including them in the join would create false matches (e.g., all rows with `DerivedSophisId = ""` would match all rows with `fissnumber = ""`), potentially producing a massive Cartesian explosion on the empty-string key.

**Step 5 — Rename to common join names:**  
Rather than writing a dynamic join condition (`axis.DerivedSophisId == fin.fissnumber`), both sides are renamed to `_jk0`, `_jk1`, etc., and joined with a simple list. This pattern generalises cleanly across 1-key and 2-key rules without branching logic.

**Step 7 — Scoring columns:**

```python
amount_rel_diff = amount_diff / greatest(abs(SACCRMTM), 1e-9)
key_strength    = len(axis_keys)   # 1 for single-key rules, 2 for composite
```

- `amount_rel_diff` normalises the amount difference to a 0–1 scale, making scores comparable across trades of different sizes. A £100 difference on a £1M trade is very different from a £100 difference on a £200 trade.
- `key_strength` reflects how many fields were matched. A composite-key match (e.g., `SourceSystemTradeId + DerivedMasterbookId`) is intrinsically more reliable than a single-key match, even at the same priority level.

---

### Section 9 — Generate All BRD Candidates

```python
candidate_dfs = []
for rule in MATCHING_RULES:
    cand = build_candidates_for_rule(axis_core, fin_core, rule)
    candidate_dfs.append(cand)

candidates_layer1 = reduce(DataFrame.unionByName, candidate_dfs)
candidates_layer1 = candidates_layer1.cache()
```

**What it does:** Calls `build_candidates_for_rule` 15 times, then unions all resulting DataFrames into a single **candidate edges table**.

**Why `unionByName` instead of `union`:**  
`union` matches columns by position — a brittle approach if any rule ever returns columns in a different order. `unionByName` matches by column name, making the union resilient to future column reordering.

**Why cache here:**  
`candidates_layer1` is consumed twice: once in the resolution step (Section 10) and once in the explainability step (Section 18c). Without caching, both operations would re-execute all 15 joins from scratch. The cache materialises the full candidate set once on the cluster and serves both consumers from memory/disk.

**Why a loop instead of 15 explicit calls:**  
Rules are data, not code. Expressing them as a list of dicts and iterating over them means: adding a new rule requires only a new dict entry, with zero changes to the execution logic.

---

### Section 10 — 1-to-1 Resolution via Window Ranking

This is the **most important algorithmic section** in the notebook.

```python
def resolve_one_to_one(candidates):
    ordering = [priority ASC, key_strength DESC, amount_diff ASC, stable_tiebreaker ASC]

    # Pass 1: best fin per axis
    w_axis = Window.partitionBy("axis_id").orderBy(*ordering, "fin_id")
    best_per_axis = candidates.withColumn("_rn", row_number().over(w_axis)) \
                               .filter("_rn = 1")

    # Pass 2: best axis per fin (prevents fin reuse)
    w_fin = Window.partitionBy("fin_id").orderBy(*ordering, "axis_id")
    pass2 = best_per_axis.withColumn("_rn", row_number().over(w_fin)) \
                          .filter("_rn = 1")

    # Pass 3: safety re-check (handles rare displacement from Pass 2)
    resolved = pass2.withColumn("_rn", row_number().over(w_axis)).filter("_rn = 1")
    return resolved
```

**Why three passes instead of two:**

- **Pass 1** selects the single best Finstore counterpart for each Axis trade. This is equivalent to "what would this Axis trade prefer?"
- **Pass 2** resolves the conflicts where multiple Axis trades each preferred the same Finstore trade. Only the highest-priority Axis trade keeps it.
- **Pass 3** is a safety net. In rare edge cases, Pass 2 can displace an Axis trade's chosen Finstore counterpart, which might cause that Axis trade to have no match. A third pass re-enforces Axis uniqueness.

**Why this replicates the Pandas waterfall:**  
The Pandas waterfall achieves "if a trade matches at rule 1, it cannot match at rule 2" by physically removing it from the pool. Window ranking achieves the same outcome by assigning `priority=1` to all Rule 1 candidates and `priority=2` to all Rule 2 candidates, then `ORDER BY priority ASC`. The lower priority number always wins in the ranking, so Rule 1 matches are preferred over Rule 2 matches — without any physical pool mutation.

**Ordering rationale:**

| Order | Column | Direction | Reason |
|---|---|---|---|
| 1st | `priority` | ASC | BRD waterfall order — lower = higher priority rule |
| 2nd | `key_strength` | DESC | More fields matched = more confident identification |
| 3rd | `amount_diff` | ASC | Closest amount = best quality match |
| 4th | `fin_id` / `axis_id` | ASC | Stable deterministic tiebreaker — reproducible results across runs |

---

### Section 11 — Compute Unmatched Pools for Greedy

```python
axis_unmatched = axis_core.join(brd_matches.select("axis_id"), on="axis_id", how="left_anti")
fin_unmatched  = fin_core.join(brd_matches.select("fin_id"),   on="fin_id",  how="left_anti")
```

**What it does:** Uses `left_anti` joins to compute the set of Axis and Finstore records not matched in Layer 1.

**Why `left_anti` instead of `NOT IN` or filtering:**  
Spark's `left_anti` join is a first-class, highly optimised operation. It works like a semi-join — it never materialises the matched side into the output, only uses it as a filter. `NOT IN` with a subquery or Python-side filtering would either push the matched IDs to the driver (memory risk) or generate a less efficient plan.

**Normalising counterparty:**  
```python
axis_unmatched = axis_unmatched.withColumn("cpty_str", F.trim(F.col("CounterpartyId").cast("string")))
fin_unmatched  = fin_unmatched.withColumn("cpty_str",  F.trim(F.col("counterpartyid").cast("string")))
```
Counterparty IDs from different systems may have trailing whitespace or inconsistent capitalisation. Trimming and casting to string before the greedy join prevents missed matches from cosmetic formatting differences.

---

### Section 12 — Greedy Strategy 1: Amount + Counterparty (1%)

```python
greedy1_candidates = (
    axis_unmatched.alias("a")
    .join(fin_unmatched.alias("f"), on=(col("a.cpty_str") == col("f.cpty_str")))
    .filter(col("amount_diff") <= col("tolerance"))   # 1% of Axis amount
)
```

**What it does:** Matches remaining unmatched trades using two conditions simultaneously: same counterparty AND amounts within 1% of each other.

**Why block on counterparty:**  
Without a blocking key, Strategy 1 would need to compare every unmatched Axis row against every unmatched Finstore row — a full cross-join. For millions of unmatched records, this is computationally intractable. Blocking on `cpty_str` ensures each Axis record is only compared against Finstore records with the **same counterparty**, reducing the comparison space by a factor of (number of distinct counterparties).

**Why 1% tolerance:**  
Amounts can differ slightly between systems due to:
- FX rate rounding (different GBP conversion snapshots)
- Accrual differences (day-count conventions)
- Booking time differences

1% is a deliberately generous tolerance to capture genuine matches while preventing false positives. After matching, `amount_rel_diff` is stored on every match row so downstream consumers can see exactly how much the amounts differed.

**Resolution:** The same `resolve_one_to_one()` function is reused. Greedy candidates use `priority=16`, ensuring they are always outranked by any BRD deterministic match (priorities 1–15) if a trade somehow appears in both candidate sets.

---

### Section 13 — Greedy Strategy 2: Amount Only (0.1%)

```python
# Create amount buckets
axis_remaining_s2 = axis_remaining_s2.withColumn(
    "amount_bucket", (F.floor(col(AXIS_AMOUNT_COL) / BUCKET_SIZE) * BUCKET_SIZE).cast("long")
)

# Expand each Axis record to 3 bucket rows (bucket-1, bucket, bucket+1)
bucket_offsets = spark.createDataFrame([(-BUCKET_SIZE,), (0,), (BUCKET_SIZE,)], ["_offset"])
axis_expanded  = axis_remaining_s2.crossJoin(F.broadcast(bucket_offsets))
                                   .withColumn("search_bucket", col("amount_bucket") + col("_offset"))

# Join Finstore on bucket
greedy2_candidates = axis_expanded.join(fin_remaining_s2,
    on=(col("a.search_bucket") == col("f.amount_bucket")))
.filter(col("amount_diff") <= col("tolerance"))   # 0.1% strict
```

**What it does:** Matches remaining unmatched trades based on amount similarity alone, with a stricter 0.1% tolerance.

**Why amount-bucket blocking:**  
Without the counterparty blocking key available, a naive approach would cross-join all remaining records. The **bucket expansion trick** avoids this:

1. Each amount is mapped to a bucket: `bucket = floor(amount / 1000) * 1000`
2. Each Axis trade expands to **3 rows** (buckets: `b-1000`, `b`, `b+1000`)
3. Finstore is joined on its single bucket

This ensures that any two amounts within ±1000 will share at least one bucket value, so **no genuine match is missed**, while the comparison space is divided by ~(range of amounts / bucket_size) compared to a cross-join.

**Why `crossJoin` on the 3-row offset table is safe:**  
The offset table has exactly 3 rows. `F.broadcast()` sends it to every worker without a shuffle. The resulting expansion is exactly 3× the input size — manageable and predictable.

**Why `.dropDuplicates(["axis_id", "fin_id"])`:**  
A pair whose amounts land in the same bucket at two different offsets (e.g., a pair where `amount_bucket_axis = amount_bucket_fin` matches on offset 0 and also on offset +1000) would otherwise appear twice. Deduplication before resolution prevents redundant candidates.

**Strict 0.1% tolerance:**  
With no counterparty anchor, a larger tolerance would generate too many false positives (especially for common notional amounts like round numbers). 0.1% is tight enough to be meaningful while still capturing legitimate FX/rounding discrepancies.

---

### Section 14 — Greedy Layer Summary

Simple aggregation cell: adds `greedy1_count + greedy2_count = total_greedy` for use in the final report. No Spark operations beyond what was already computed.

---

### Section 15 — Final Consolidation

```python
all_matches = brd_matches.unionByName(greedy1_matches).unionByName(greedy2_matches)

final_unmatched_axis = axis_core.join(all_matches.select("axis_id"), "axis_id", "left_anti")
final_unmatched_fin  = fin_core.join( all_matches.select("fin_id"),  "fin_id",  "left_anti")
```

**What it does:** Unions all three match DataFrames into `all_matches`, then computes final unmatched sets.

**Why the schema is guaranteed consistent:**  
All three DataFrames were produced with the **same column list** in the same order:
`axis_id, fin_id, priority, category, brd_priority, description, amount_diff, amount_rel_diff, key_strength, MatchLayer, run_id, batch_id, rule_version, match_timestamp`

`unionByName` (rather than positional `union`) provides an additional safety guarantee: if any future change reorders columns in one path, the union still works correctly.

**Why `all_matches` is cached:**  
It is consumed by the enrichment join (Section 16), the system breakdown (Section 17), and the DQ validation (Section 18b). Caching avoids three independent re-executions of the full matching pipeline.

---

### Section 16 — Enrichment: Join Back Wide Columns

```python
axis_wide_renamed = axis_wide  # rename all non-id cols to {col}_Axis
fin_wide_renamed  = fin_wide   # rename all non-id cols to {col}_Finstore

matches_enriched = all_matches
    .join(axis_wide_renamed, on="axis_id", how="left")
    .join(fin_wide_renamed,  on="fin_id",  how="left")
    .withColumn("Variance", col(f"{AXIS_AMOUNT_COL}_Axis") - col(f"{FIN_AMOUNT_COL}_Finstore"))
```

**What it does:** Joins the full 100+ column wide tables back onto the matched set using `axis_id` / `fin_id`.

**Why this is the only wide join in the notebook:**  
Every join before this point operated on ~10–15 column core schemas. This is intentional — all the expensive shuffle operations (15 BRD joins, 2 greedy joins) involve narrow rows. This single enrichment join involves wide rows, but it operates on `all_matches` which is already a small fraction of the original input (typically 80–95% of Axis rows, but now in a single known-small DataFrame). Even at wide schema, one join on a small set is far cheaper than 15 joins on the full set.

**Column namespacing:**  
All Axis wide columns are suffixed `_Axis`, all Finstore wide columns `_Finstore`. This prevents collisions when the two wide schemas have overlapping column names (e.g., both have `TradeDate`, `Currency`, `CounterpartyId`).

---

### Section 17 — Matches by System Breakdown

Groups `brd_matches` and `greedy1_matches.unionByName(greedy2_matches)` by `SourceSystemName` to show the distribution of matches per source system. This is a pure aggregation — no joins, no new computation.

**Operational use:** Identifies if a specific system is consistently underperforming (e.g., only 20% match rate while others are at 90%), which would indicate a data quality or mapping issue with that system's feed.

---

### Section 18 — Remaining Unmatched by System

Groups `final_unmatched_axis` by `SourceSystemName`. Equivalent diagnostic to Section 17 but from the unmatched perspective.

---

### Section 18b — Data Quality Validation

```python
def validate_dataframe(df, name, checks):
    rows = []
    for desc, expr in checks:
        cnt = df.filter(expr).count()
        rows.append((name, desc, cnt))
    return spark.createDataFrame(rows, ["dataset", "check", "violation_count"])
```

**What it does:** Runs a configurable list of null/range checks against the core input DataFrames and the `all_matches` output, then prints a violation summary.

**Checks applied:**

| Dataset | Check |
|---|---|
| `axis_core` | null `axis_id`, null `CounterpartyId`, null/negative `SACCRMTM`, null `ReconSubProduct` |
| `fin_core` | null `fin_id`, null `counterpartyid`, null/negative `gbpequivalentamount` |
| `all_matches` | null `axis_id`, null `fin_id`, null `description`, null `run_id`, `amount_rel_diff > 1.0` |

**Why `amount_rel_diff > 1.0` is a useful check:**  
A relative difference greater than 100% means the amounts differ by more than 100% — the smaller amount is less than half the larger. This should never happen for a genuine deterministic match (the join keys must agree), and is a strong signal of a data quality issue or a misfire in the greedy strategies.

**Why run DQ after matching, not before:**  
Pre-match DQ validation can be useful but is expensive on full datasets. Running DQ on both the inputs (already cached) and the output (already computed) at this point is effectively free — the DataFrames are already in memory.

---

### Section 18c — Explainability: Unmatched Reason Breakdown

```python
axis_candidates_ever = candidates_layer1.select("axis_id").distinct()

axis_unmatched_reasons = final_unmatched_axis
    .join(axis_candidates_ever, on="axis_id", how="left")
    .withColumn("unmatched_reason",
        F.when(axis_candidates_ever["axis_id"].isNotNull(),
               "candidate_existed_but_consumed_by_higher_priority")
         .otherwise("no_candidate_key_found"))
```

**What it does:** Classifies every unmatched trade into one of two mutually exclusive categories:

| Reason | Meaning |
|---|---|
| `no_candidate_key_found` | The trade's join keys (e.g., `SourceSystemTradeId`) had no match in any Finstore row across all 15 rules. This is a **data gap** — the trade genuinely does not exist in Finstore with a matching key. |
| `candidate_existed_but_consumed_by_higher_priority` | The trade did appear as a candidate in at least one rule, but its preferred Finstore counterpart was taken by a higher-priority (lower number) Axis trade first. This is a **competition loss** — the trade could potentially be matched with a different rule or a more lenient greedy strategy. |

**Why this distinction matters for operations:**  
The two categories require completely different remediation actions. `no_candidate_key_found` trades need investigation into the source system feed or the key derivation logic. `consumed_by_higher_priority` trades indicate the resolution algorithm made a deterministic choice — reviewing whether the priority ordering is correct is the relevant action.

---

### Section 19 — Save Results

```python
brd_matches.write.format("delta").mode("overwrite").save(...)
matches_enriched.write.format("delta").mode("overwrite").save(...)
unmatched_axis_enriched.write.format("delta").mode("overwrite").save(...)
```

**What it does:** Persists all output DataFrames as Delta tables.

**Why Delta over Parquet or CSV:**

| Format | Issue |
|---|---|
| CSV | No schema, no transactional writes, no time-travel, slow to read back |
| Parquet | Good columnar format but no ACID transactions, no `MERGE`, no schema evolution |
| Delta | ACID transactions, time-travel (rollback to previous version), schema enforcement, `MERGE` for upserts, efficient metadata reads |

For regulatory capital reporting, **time-travel** is particularly valuable: if an error is discovered post-report, you can retrieve the exact matched set as it existed at submission time.

**ZORDER guidance (commented out, production-ready):**

```python
# spark.sql(f"OPTIMIZE delta.`{OUTPUT_DIR}/matched_all_combined` ZORDER BY (axis_id, fin_id, priority)")
```

ZORDER co-locates rows with similar values on disk. If downstream queries frequently filter by `axis_id` or `priority`, ZORDERed files skip entire file groups without reading them. On very large tables this reduces query time from hours to minutes.

---

### Section 20 — Summary Report

Generates a human-readable text report including run metadata (`run_id`, `batch_id`, `rule_version`), match counts at every layer, performance notes, and DQ status. Saved as a text file alongside the Delta outputs for archival.

---

### Section 21 — Cleanup

```python
for df in [axis_core, fin_core, candidates_layer1, brd_matches, ...]:
    df.unpersist()
```

**What it does:** Explicitly releases all cached DataFrames from Spark's block store.

**Why this matters:** Spark's block store has a finite size (configurable, typically a fraction of executor memory). If a long-running cluster is used for multiple jobs, uncleaned caches from a previous job consume block store space, slowing down subsequent jobs. Explicit unpersist is good practice even if the cluster is dedicated, because it makes resource consumption predictable and auditable.

---

## 4. All 15 BRD Rules — Detailed Reference

### Rule Execution Model

Every rule follows the same four-step execution model inside `build_candidates_for_rule`:

1. **Filter** — Apply system or feature constraints to reduce rows before the join
2. **Project** — Select only `axis_id + amount + join keys` from each side
3. **Join** — Equi-join on the (renamed) key columns
4. **Score** — Compute `amount_diff`, `amount_rel_diff`, `key_strength`

The join result is **not resolved** inside the function — it simply adds candidate edges. Resolution happens globally after all 15 rules have contributed their candidates.

---

### SOPHIS Rules (P1–P3)

These three rules apply **only to Axis trades from SOPHIS systems** (`filter_sophis_only=True`):
`SOPHIS-LONDON`, `SOPHIS-NEWYORK`, `SOPHIS-TOKYO`, `SOPHISFX-LONDON`.

SOPHIS trade IDs are formatted as `PREFIX-SYSTEM-{sophisId}`. The derived field `DerivedSophisId` is the third hyphen-segment extracted in the Silver layer.

---

#### Priority 1 — SOPHIS #1: `DerivedSophisId ↔ fissnumber`

| | Axis | Finstore |
|---|---|---|
| **Key** | `DerivedSophisId` | `fissnumber` |
| **System filter** | SOPHIS only | None |
| **Key strength** | 1 |

**Rationale:** `fissnumber` in Finstore is SOPHIS's own internal trade reference. For SOPHIS-originated trades, this is the most direct and reliable linkage. It is intentionally the **highest-priority rule** so that SOPHIS trades that can be resolved with certainty are locked in first, preventing them from being mistakenly captured by lower-priority cross-system rules.

**Example:**  
Axis `TRD-LON-S001` (SOPHIS-LONDON) → `DerivedSophisId = "S001"` → matches Finstore row where `fissnumber = "S001"`.

---

#### Priority 2 — SOPHIS #2: `DerivedSophisId + BookId ↔ fissnumber + tradingsystembook`

| | Axis | Finstore |
|---|---|---|
| **Keys** | `DerivedSophisId`, `BookId` | `fissnumber`, `tradingsystembook` |
| **System filter** | SOPHIS only | None |
| **Key strength** | 2 |

**Rationale:** Adds `BookId ↔ tradingsystembook` as a second disambiguation key. Used when `fissnumber` alone is not unique — e.g., when the same SOPHIS trade ID exists across multiple books (can happen with transferred or re-booked positions). The composite key makes this match more specific than P1 but also more restrictive, which is why it ranks second.

**Example:**  
Axis `TRD-NY-S002` (SOPHIS-NEWYORK), `BookId="BOOK-SOPHIS-2"` → matches Finstore `fissnumber="S002"`, `tradingsystembook="BOOK-SOPHIS-2"`.

---

#### Priority 3 — SOPHIS #3: `DerivedSophisId ↔ tradeid`

| | Axis | Finstore |
|---|---|---|
| **Key** | `DerivedSophisId` | `tradeid` |
| **System filter** | SOPHIS only | None |
| **Key strength** | 1 |

**Rationale:** Some Finstore records store the SOPHIS ID in `tradeid` rather than `fissnumber`. This rule catches those cases. Ranked lower than P1/P2 because matching `DerivedSophisId` to `tradeid` is a cross-field match (less direct than matching to the dedicated `fissnumber` field), and there is a higher theoretical risk of collision with non-SOPHIS `tradeid` values.

**Example:**  
Axis `TRD-TKY-S003` (SOPHIS-TOKYO) → `DerivedSophisId = "S003"` → matches Finstore `tradeid = "S003"`.

---

### OTC Rules (P4–P13)

OTC rules apply to **all in-scope Axis systems** (no `filter_sophis_only`). They cover the majority of trades.

---

#### Priority 4 — OTC #1: `SourceSystemTradeId ↔ tradeid`

| | Axis | Finstore |
|---|---|---|
| **Key** | `SourceSystemTradeId` | `tradeid` |
| **Key strength** | 1 |

**Rationale:** The "quick win" rule. For most OTC systems, the trade ID in Axis is exactly the same string stored as `tradeid` in Finstore. This is the simplest and most reliable linkage when the two systems share a common trade identifier. Ranked P4 (highest OTC priority) because it is the most direct match — if this rule fires, the match confidence is very high.

**Example:**  
Axis `OTC-TRADE-0001` (IFLOW-EUROPE) → matches Finstore `tradeid = "OTC-TRADE-0001"`.

---

#### Priority 5 — OTC #2: `SourceSystemTradeId + DerivedMasterbookId ↔ tradeid + masterbookid`

| | Axis | Finstore |
|---|---|---|
| **Keys** | `SourceSystemTradeId`, `DerivedMasterbookId` | `tradeid`, `masterbookid` |
| **Requires SDS** | Yes (`requires_derived_masterbook=True`) |
| **Key strength** | 2 |

**Rationale:** When `SourceSystemTradeId ↔ tradeid` is not unique (the same trade ID exists in multiple books), adding `DerivedMasterbookId ↔ masterbookid` disambiguates. Skipped entirely when `DerivedMasterbookId` is empty (SDS mapping not provided) to avoid false negatives from joining an empty string against populated `masterbookid` values.

---

#### Priority 6 — OTC #3: `SourceSystemTradeId ↔ alternatetradeid1`

| | Axis | Finstore |
|---|---|---|
| **Key** | `SourceSystemTradeId` | `alternatetradeid1` |
| **Key strength** | 1 |

**Rationale:** Some systems (e.g., SUMMIT-LONDON) book trades into Finstore with the originating system's ID stored in an alternate ID field rather than the primary `tradeid`. This rule catches those. Ranked lower than P4 because alternate IDs have a higher probability of collision with other trade IDs from different systems.

**Example:**  
Axis `SUM-TRADE-0002` (SUMMIT-LONDON) → matches Finstore `alternatetradeid1 = "SUM-TRADE-0002"`.

---

#### Priority 7 — OTC #4: `SourceSystemTradeId + DerivedMasterbookId ↔ alternatetradeid1 + masterbookid`

The composite-key version of P6. Same disambiguation rationale as P5 relative to P4. Skipped when SDS unavailable.

---

#### Priority 8 — OTC #5: `DerivedSophisId ↔ fissnumber`

| | Axis | Finstore |
|---|---|---|
| **Key** | `DerivedSophisId` | `fissnumber` |
| **System filter** | None (all OTC systems) |
| **Key strength** | 1 |

**Rationale:** Identical key logic to P1 but applied to **all** OTC systems (not SOPHIS-only). This covers cases where a non-SOPHIS system (e.g., `SOPHISFX-LONDON` when scope is not SOPHIS-filtered) has a trade ID that follows the SOPHIS format. Ranked P8 to ensure SOPHIS-specific rules (P1–P3) have priority over this broader cross-system application.

**Example:**  
Axis `FX-LON-S004` (SOPHISFX-LONDON) → `DerivedSophisId = "S004"` → matches Finstore `fissnumber = "S004"`.

---

#### Priority 9 — OTC #6: `DerivedSophisId + BookId ↔ fissnumber + tradingsystembook`

Composite-key version of P8 (same disambiguation logic as P2 but applied to all systems).

---

#### Priority 10 — OTC #7: `DerivedSophisId ↔ tradeid`

Cross-system equivalent of P3. Applied to all OTC systems (not SOPHIS-filtered).

---

#### Priority 11 — OTC #8: `DerivedDelta1Id ↔ tradeid`

| | Axis | Finstore |
|---|---|---|
| **Key** | `DerivedDelta1Id` | `tradeid` |
| **Key strength** | 1 |

**Rationale:** DELTA1 systems encode their canonical trade ID in the third hyphen-segment of `SourceSystemTradeId` (same structure as SOPHIS). This derived ID appears in Finstore's `tradeid` field. Note: when `EXCLUDE_SOPHIS_DELTA1=True`, DELTA1 trades are removed from scope in Section 5, so this rule fires zero candidates. It is active when scope exclusion is disabled.

---

#### Priority 12 — OTC #9: `SourceSystemTradeId ↔ alternatetradeid2`

| | Axis | Finstore |
|---|---|---|
| **Key** | `SourceSystemTradeId` | `alternatetradeid2` |
| **Key strength** | 1 |

**Rationale:** A third Finstore ID field for systems that store the originating trade ID in the second alternate field. Ranked lower than P6 because `alternatetradeid2` is used by fewer systems and has a higher collision risk.

**Example:**  
Axis `GCD-TRADE-0005` (GCD-NEWYORK) → matches Finstore `alternatetradeid2 = "GCD-TRADE-0005"`.

---

#### Priority 13 — OTC #10: `SourceSystemTradeId + DerivedMasterbookId ↔ alternatetradeid2 + masterbookid`

Composite-key version of P12. Skipped when SDS unavailable.

---

### ETD Rules (P14–P15)

ETD (Exchange Traded Derivatives) trades are identified by instrument rather than by individual trade ID. These rules apply to Axis trades classified as `ReconSubProduct = "ETD"` (systems in `ETD_SYSTEMS`).

---

#### Priority 14 — ETD #1: `SourceSystemInstrumentId + DerivedMasterbookId ↔ instrumentid + masterbookid`

| | Axis | Finstore |
|---|---|---|
| **Keys** | `SourceSystemInstrumentId`, `DerivedMasterbookId` | `instrumentid`, `masterbookid` |
| **Requires SDS** | Yes (`requires_derived_masterbook=True`) |
| **Key strength** | 2 |

**Rationale:** For ETD, many trades on the same instrument are booked identically — the instrument ID plus the book uniquely identifies a position aggregate. The composite key is the most specific linkage possible and is therefore ranked first among ETD rules. Skipped when SDS unavailable.

---

#### Priority 15 — ETD #2: `SourceSystemInstrumentId ↔ instrumentid`

| | Axis | Finstore |
|---|---|---|
| **Key** | `SourceSystemInstrumentId` | `instrumentid` |
| **Key strength** | 1 |

**Rationale:** Fallback when SDS is not available (or when P14 finds no match). Instrument ID alone is less specific than the composite key (multiple books may hold positions in the same instrument), but it is still a reliable linkage for ETD trades.

**Example:**  
Axis `ETD-TRADE-7001` (ODH-GMI-LONDON), `SourceSystemInstrumentId="INST-XYZ-001"` → matches Finstore `instrumentid = "INST-XYZ-001"`.

---

## 5. Greedy Strategies — Detailed Reference

Greedy strategies are applied **only to trades unmatched after all 15 BRD rules**. They use probabilistic/fuzzy matching and are therefore given lower priority values (P16, P17) and stored with `key_strength=0`.

### Strategy 1 — Amount + Counterparty (1% tolerance)

**Applicable when:** A trade has no matching key in any BRD rule, but the counterparty ID is present and reliable.

**Mechanism:**
1. Block join on `counterpartyid` (eliminates cross-counterparty comparisons)
2. Filter: `abs(SACCRMTM - gbpequivalentamount) ≤ 0.01 × abs(SACCRMTM)`
3. Resolve 1-to-1 with the same window ranking function

**Key insight:** Two trades with the same counterparty and amounts within 1% of each other are almost certainly the same economic position recorded with slight discrepancies. The counterparty anchor prevents accidental matches between unrelated trades at similar amounts.

### Strategy 2 — Amount Only (0.1% strict tolerance)

**Applicable when:** No counterparty anchor is available or no match was found in Strategy 1.

**Mechanism:**
1. Bucket blocking: `bucket = floor(amount / 1000) * 1000`
2. Expand Axis to ±1 bucket (3 rows per record)
3. Join on bucket, filter: `abs(diff) ≤ 0.001 × abs(SACCRMTM)`
4. Deduplicate, resolve 1-to-1

**Why tighter tolerance (0.1% vs 1%):** Without a counterparty anchor, there is a higher risk of false positives — especially at common notional amounts (£1M, £5M). The stricter tolerance reduces this risk.

---

## 6. End-to-End Data Flow Diagram

```
CSV Files (axis_sample_poc.csv, finstore_sample_poc.csv)
         │
         ▼  Section 4 — Bronze ingestion + metadata stamps
  df_axis_full ──── _ingest_timestamp, _source_file, _batch_id
  df_finstore_full
         │
         ▼  Section 5 — Scope exclusion
  df_axis  (SOPHIS/DELTA1 removed)
         │
         ▼  Section 6 — Silver derivations (no UDFs)
  df_axis  + axis_id, DerivedSophisId, DerivedDelta1Id, ReconSubProduct
  df_finstore + fin_id
         │
         ▼  Section 7 — Core/Wide split
  axis_core (11 cols)  ────────────────────────────────────────────────┐
  fin_core  (10 cols)  ───────────────────────────────────┐            │
  axis_wide (all cols)  ──────────────────────────────────│────────────│──► Section 16
  fin_wide  (all cols)  ──────────────────────────────────│────────────│──► Section 16
         │                                                │            │
         ▼  Section 8/9 — 15 BRD candidate joins          │            │
  candidates_layer1  ←─── 15 × build_candidates_for_rule │            │
  (axis_id, fin_id, priority, amount_diff, key_strength)  │            │
         │                                                │            │
         ▼  Section 10 — 3-pass window ranking            │            │
  brd_matches (1-to-1)                                    │            │
         │                                                │            │
         ▼  Section 11 — Anti-join                        │            │
  axis_unmatched ◄─── axis_core ─────────────────────────┘            │
  fin_unmatched  ◄─── fin_core                                         │
         │                                                             │
         ▼  Section 12/13 — Greedy blocked joins                       │
  greedy1_matches (cpty block, 1%)                                     │
  greedy2_matches (bucket block, 0.1%)                                 │
         │                                                             │
         ▼  Section 15 — Union + final anti-join                       │
  all_matches                                                          │
  final_unmatched_axis                                                 │
  final_unmatched_fin                                                  │
         │                                                             │
         ▼  Section 16 — Wide enrichment ◄──────────────────────────────┘
  matches_enriched          (all_matches + 100+ cols each side)
  unmatched_axis_enriched
  unmatched_fin_enriched
         │
         ▼  Section 18b/c — DQ + Explainability
  dq_report
  axis_unmatched_reasons
  fin_unmatched_reasons
         │
         ▼  Section 19 — Delta writes
  matched_brd_layer, matched_greedy_layer, matched_all_combined
  unmatched_axis_final, unmatched_finstore_final
```

---

## 7. Key Efficiency Decisions — Summary Table

| Decision | Pandas Notebook | PySpark Notebook | Efficiency Gain |
|---|---|---|---|
| **Rule execution** | 15 sequential join + anti-join cycles | 15 candidate edge generations → single union → single window rank | Eliminates 14 anti-join shuffles |
| **Column payload** | Full 100+ col schema throughout | Core schema (~11 cols) for all matching; wide join only at end | ~10–17× reduction in shuffle I/O |
| **SOPHIS ID extraction** | Python `str.split()` UDF per row | `F.split() + F.element_at()` native Spark SQL | JVM-native execution; no Python serialisation |
| **Greedy blocking** | `groupby + iterrows` loops | Equi-join on `cpty_str` (Strategy 1) / bucket expansion (Strategy 2) | Scales from O(n²) to O(n × candidates_per_key) |
| **1-to-1 enforcement** | Pool mutation (remove matched sets) | 3-pass window ranking | Single shuffle; fully deterministic |
| **Skew handling** | N/A (single-machine) | AQE `skewJoin.enabled` + bucket ±1 expansion | Prevents single-task bottlenecks on hot keys |
| **Caching strategy** | Pandas DataFrames always in memory | Explicit `.cache()` only on reused DataFrames (core tables, candidates) | Avoids unnecessary memory pressure on wide tables |
| **Output format** | CSV | Delta Lake (ACID, time-travel, ZORDER) | Queryable, rollback-capable, schema-enforced |
| **Auditability** | No lineage columns | `run_id`, `batch_id`, `rule_version`, `match_timestamp` on every row | Full run traceability for regulatory audit |
| **Data quality** | None | `validate_dataframe()` null/range checks post-match | Catches data contract violations before Gold promotion |
| **Explainability** | None | `unmatched_reason` classification on every unmatched trade | Enables targeted remediation vs. broad investigation |
