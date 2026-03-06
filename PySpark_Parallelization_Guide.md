# PySpark Parallelisation Guide
## Derivatives Matching Engine — Theory, Practice & Databricks Monitoring

---

## Table of Contents
1. [Cluster Architecture](#1-cluster-architecture)
2. [Partitions — The Unit of Parallel Work](#2-partitions--the-unit-of-parallel-work)
3. [Operations: Narrow vs Wide (Shuffle)](#3-operations-narrow-vs-wide-shuffle)
4. [How Each Notebook Section Parallelises](#4-how-each-notebook-section-parallelises)
5. [Spark Configurations Applied in This Notebook](#5-spark-configurations-applied-in-this-notebook)
6. [Checking Parallelisation in Databricks — Step by Step](#6-checking-parallelisation-in-databricks--step-by-step)
7. [What to Look For and How to Diagnose Issues](#7-what-to-look-for-and-how-to-diagnose-issues)
8. [Common Problems and Fixes](#8-common-problems-and-fixes)

---

## 1. Cluster Architecture

### Driver vs Workers

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS CLUSTER                           │
│                                                                     │
│  ┌─────────────┐                                                    │
│  │   DRIVER    │  ← Your notebook runs here (1 node)               │
│  │  (1 node)   │  ← Orchestrates work, collects final results       │
│  │             │  ← Holds: Python variables, configs, print output  │
│  └──────┬──────┘                                                    │
│         │  Distributes tasks                                        │
│  ┌──────┴──────────────────────────────────────────────┐            │
│  │                WORKERS (2–10 nodes)                  │            │
│  │                                                      │            │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │            │
│  │  │ Worker 1 │  │ Worker 2 │  │ Worker 3 │  ...     │            │
│  │  │ 16 cores │  │ 16 cores │  │ 16 cores │          │            │
│  │  │  122 GB  │  │  122 GB  │  │  122 GB  │          │            │
│  │  └──────────┘  └──────────┘  └──────────┘          │            │
│  └─────────────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
```

**With the notebook cluster config (10 workers × 16 cores):**
- Total parallel tasks: **160 cores running simultaneously**
- Total cluster RAM: **~1.22 TB**

### What Runs Where

| Code | Where | Parallel? |
|------|-------|-----------|
| Python variables (`MATCHING_RULES`, configs) | Driver | ❌ Single machine |
| `for rule in MATCHING_RULES:` loop | Driver | ❌ Sequential |
| `df.join(...)`, `df.filter(...)` | Workers | ✅ Fully distributed |
| `Window.partitionBy(...)` | Workers | ✅ Parallel per partition |
| `df.count()`, `df.show()` | Workers compute → Driver collects | ✅ Parallel compute |
| `print(...)` | Driver only | ❌ Single machine |

---

## 2. Partitions — The Unit of Parallel Work

### What a Partition Is

When Spark reads a CSV or processes a DataFrame, it splits the data into **partitions** — independent chunks of rows that can be processed on different cores simultaneously.

```
axis_sample_poc.csv (3.29M rows)
        ↓ spark.read.csv()
┌────────┬────────┬────────┬────────┬─────┬──────────┐
│ Part 0 │ Part 1 │ Part 2 │ Part 3 │ ... │ Part 319 │
│ ~10K   │ ~10K   │ ~10K   │ ~10K   │     │ ~10K     │
│  rows  │  rows  │  rows  │  rows  │     │  rows    │
└────────┴────────┴────────┴────────┴─────┴──────────┘

Each partition → assigned to 1 core on 1 worker
160 cores → 160 partitions running at the same time
320 total partitions → each core processes 2 partitions sequentially
```

### This Notebook's Partition Setting

```python
# In Cell 3 (SparkSession config)
spark.conf.set("spark.sql.shuffle.partitions", "320")
# Rule of thumb: 2–3× total cores (10 workers × 16 cores = 160 cores → 320 partitions)
```

After every **shuffle operation** (join, groupBy, orderBy across the whole dataset), Spark creates 320 new partitions. AQE (Adaptive Query Execution) then coalesces empty or very small ones automatically.

---

## 3. Operations: Narrow vs Wide (Shuffle)

### Narrow Transformations — Fast (No Data Movement)

Each worker processes its own partitions independently. No network traffic.

```python
# Cell 5 — Scope filter: each worker filters its own partitions
df_axis = df_axis.filter(~F.col("SourceSystemName").isin(OUT_OF_SCOPE_SYSTEMS))

# Cell 14 — Derivations: each worker computes on its own rows
df_axis = df_axis.withColumn("amount_diff",
    F.abs(F.col(AXIS_AMOUNT_COL) - F.col(FIN_AMOUNT_COL)))

# Cell 14 — DerivedSophisId: each worker applies string split to its rows
df_axis = df_axis.withColumn("DerivedSophisId",
    F.element_at(F.split(F.col("SourceSystemTradeId"), "-"), 3))
```

```
Worker 1: [Part 0, Part 1, ...]  → apply filter → [filtered Part 0, filtered Part 1, ...]
Worker 2: [Part 16, Part 17, ...] → apply filter → [filtered Part 16, ...]
No data crosses worker boundaries ✅
```

### Wide Transformations — Requires Shuffle (Data Moves Between Workers)

Data must be **redistributed** so all rows with the same key end up on the same worker.

#### Example: `safe_rule_join` (Cell 19)

```python
joined = a_sel.join(f_sel, on=join_cols, how="inner")
```

```
BEFORE shuffle (data randomly distributed):
  Worker 1: axis rows with SourceSystemTradeId = [A, C, B, D]
  Worker 2: axis rows with SourceSystemTradeId = [C, A, E, B]
  Worker 3: fin  rows with tradeid             = [B, D, A, C]

SHUFFLE: re-partition both DataFrames by the join key hash

AFTER shuffle (matching keys co-located):
  Worker 1: axis[tradeid=A, C] + fin[tradeid=A, C]  → local join ✅
  Worker 2: axis[tradeid=B, D] + fin[tradeid=B, D]  → local join ✅
  Worker 3: axis[tradeid=E]   + fin[tradeid=E]       → local join ✅
```

#### Example: `Window.partitionBy` — `resolve_matches()` (Cell 25)

```python
w_axis = Window.partitionBy("axis_id").orderBy(...)
current = current.withColumn("_rn_axis", F.row_number().over(w_axis))
```

All rows for the same `axis_id` must be on the same worker to rank them:

```
SHUFFLE: move all rows with the same axis_id to the same partition

Worker 1 gets all rows where axis_id IN [1001, 1002, 1003, ...]
  → ranks each group independently
  → keeps only row_number == 1 per axis_id

Worker 2 gets all rows where axis_id IN [2001, 2002, 2003, ...]
  → ranks each group independently
```

### Broadcast Join — No Shuffle for Small Tables

```python
# Cell 14 — SDS mapping (3 rows → broadcast, no shuffle needed)
df_axis = df_axis.join(
    F.broadcast(df_sds_mapping),  # ← entire table sent to every worker
    df_axis["SourceSystemTradeId"] == F.col("_sds_trade_id"),
    how="left",
)
```

```
WITHOUT broadcast (shuffle join):          WITH broadcast (no shuffle):
  Driver sends SDS to shuffle buffer   →     Driver sends 3 SDS rows to ALL workers
  Workers exchange axis rows           →     Each worker joins locally
  Expensive network traffic            →     Zero shuffle cost ✅
```

**Auto-broadcast:** The config `spark.sql.autoBroadcastJoinThreshold = 256 MB` means Spark will automatically broadcast any table smaller than 256 MB — including `axis_core` if it fits after filtering.

---

## 4. How Each Notebook Section Parallelises

### Section 9 — Sequential Waterfall Loop (Cell 21)

```python
# Driver: sequential — processes rules one at a time
for rule in sorted(MATCHING_RULES, key=lambda r: r["priority"]):
    cand = safe_rule_join(axis_core, fin_core, rule, matched_axis_ids)
    # ↑ Workers: inner join + filter + top_k window — fully parallel
```

| Level | Behaviour |
|-------|-----------|
| Loop over 15 rules | Sequential on driver |
| Anti-join `matched_axis_ids` per rule | Parallel on workers |
| Inner join on keys | Parallel on workers (shuffle) |
| `top_k` window ranking | Parallel on workers (shuffle) |
| `matched_axis_ids.union().distinct()` | Parallel on workers |

Each rule fires a **separate Spark job**. You'll see 15 separate jobs in the Spark UI.

### Section 7 — Core/Wide Split + `persist()` (Cell 16)

```python
axis_core = axis_core.persist(StorageLevel.MEMORY_AND_DISK)
fin_core  = fin_core.persist(StorageLevel.MEMORY_AND_DISK)
```

Spark **caches each partition in its worker's RAM**. All 15 rules + both greedy strategies read from cache instead of re-scanning the CSV:

```
Worker 1 RAM: [Part 0 of axis_core] [Part 1 of axis_core] ...
Worker 2 RAM: [Part 16 of axis_core] [Part 17 of axis_core] ...
...
If RAM full → spills to worker local SSD (DISK part of MEMORY_AND_DISK)
```

### Section 13 — Greedy Bucket Expand (Cell 34)

```python
bucket_offsets = spark.createDataFrame([(-1000,), (0,), (1000,)], ["_offset"])
axis_expanded = axis_remaining_s2.crossJoin(F.broadcast(bucket_offsets))
```

- 3-row table → broadcast (zero shuffle)  
- Each worker triples its local axis partitions (1 row → 3 rows)  
- Subsequent bucket join is perfectly hash-partitioned by `search_bucket`

```
Worker 1 partition (before):     Worker 1 partition (after crossJoin):
  [axis_id=1, bucket=5000]    →   [axis_id=1, search_bucket=4000]
                                  [axis_id=1, search_bucket=5000]
                                  [axis_id=1, search_bucket=6000]
  [axis_id=2, bucket=7000]    →   [axis_id=2, search_bucket=6000]
                                  [axis_id=2, search_bucket=7000]
                                  [axis_id=2, search_bucket=8000]
```

---

## 5. Spark Configurations Applied in This Notebook

All set in Cell 3:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "320")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(256 * 1024 * 1024))
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

| Setting | Value | Why |
|---------|-------|-----|
| `adaptive.enabled` | `true` | Lets Spark re-optimise the plan at runtime based on actual data stats |
| `coalescePartitions` | `true` | AQE merges tiny post-shuffle partitions automatically |
| `skewJoin.enabled` | `true` | AQE splits skewed partitions (e.g., if one join key has 1M rows) |
| `localShuffleReader` | `true` | Reads shuffle data locally when possible, avoids network hop |
| `shuffle.partitions` | `320` | 2× total cores (160); AQE coalesces down if many are empty |
| `autoBroadcastJoinThreshold` | `256 MB` | Tables < 256 MB are auto-broadcast (no shuffle) |
| `advisoryPartitionSizeInBytes` | `128 MB` | Target partition size after AQE coalesce |
| `delta.optimizeWrite` | `true` | Fewer, larger output files when writing Delta tables |
| `delta.autoCompact` | `true` | Automatically compact Delta files after writes |

---

## 6. Checking Parallelisation in Databricks — Step by Step

### 6.1 Spark UI — Jobs Tab

**Where:** In a running Databricks cluster, click the **Spark UI** link in the cluster details, or use the **Job** link that appears below each cell output.

**What to look at:**
1. Each `.count()`, `.show()`, or `.write` in the notebook triggers a **Spark Job**
2. Click a job to see its **Stages**
3. Click a stage to see individual **Tasks**

```
Jobs list example:
  Job 0: count at Cell 16 (axis_core materialise)     → 320 tasks
  Job 1: count at Cell 16 (fin_core materialise)      → 320 tasks
  Job 2: count at Cell 21 (safe_rule_join P1)          → 640 tasks (join = 2 stages)
  ...
  Job 16: count at Cell 25 (brd_matches)               → 320 tasks
```

**Healthy signs:**
- All tasks complete quickly (similar duration)
- No tasks show "FAILED" or "RETRY"

### 6.2 Spark UI — Stages Tab

Each stage represents a computation between two shuffles.

**What to check:**
| Metric | Healthy | Problem |
|--------|---------|---------|
| **Task Duration** | Similar across tasks (e.g., all 1–3s) | One task 100× slower = data skew |
| **Shuffle Read/Write** | Moderate (100 MB–1 GB) | Very high (10+ GB) = hot key problem |
| **GC Time** | < 5% of task duration | > 10% = memory pressure |
| **Spill (Memory)** | 0 or small | Large spill = workers are OOM |
| **Spill (Disk)** | 0 | Any disk spill = memory problem |

**How to open a stage:**
1. Click the stage link
2. Scroll to **Task Metrics Summary** (min/median/max/p75)
3. Look at the **Event Timeline** to see task distribution

### 6.3 Spark UI — SQL / DataFrame Tab

This shows the **physical plan** — how Spark actually executes your joins.

**Where:** Spark UI → SQL/DataFrame tab → click on a query

**What to look for:**

```
Example physical plan for safe_rule_join P4:

SortMergeJoin [_jk0], [_jk0], Inner          ← join type
:- Exchange hashpartitioning(_jk0, 320)       ← shuffle of axis_core
:  +- Filter (isnotnull(_jk0) AND ...)        ← key cleaning
:     +- InMemoryTableScan [axis_id, ...]     ← reading from cache ✅
+- Exchange hashpartitioning(_jk0, 320)       ← shuffle of fin_core
   +- Filter (isnotnull(_jk0) AND ...)
      +- InMemoryTableScan [fin_id, ...]      ← reading from cache ✅
```

**Good signs:**
- `InMemoryTableScan` = reading from `.persist()` cache ✅
- `BroadcastHashJoin` = table was broadcast (no shuffle) ✅
- `AdaptiveSparkPlan` wrapper = AQE is active ✅

**Warning signs:**
- `FileScan CSV` (not `InMemoryTableScan`) = cache missed, re-reading file ⚠️
- `SortMergeJoin` on very large tables = expensive shuffle happening

### 6.4 Cluster Metrics — CPU and Memory

**Where:** Databricks workspace → **Compute** → select your cluster → **Metrics** tab

**What to check:**

| Metric | Healthy | Problem |
|--------|---------|---------|
| **CPU utilisation** | 70–100% during joins | < 30% = idle workers (skew or driver bottleneck) |
| **Memory utilisation** | 40–80% | > 90% = risk of OOM / spill to disk |
| **Network I/O** | Spikes during shuffles | Sustained very high = too many shuffles |
| **Disk I/O** | Low | Spikes = cache spilling to disk (add workers) |

**Timeline:**
- You should see CPU spikes when each `safe_rule_join` runs
- Between rules, CPU drops while the driver loops to the next rule
- A long flat CPU line (near 0%) means the cluster is idle — usually the driver is executing Python

### 6.5 Ganglia / Metrics (Legacy Clusters)

On clusters with Ganglia enabled, you can also check:
- **load_one** graph: parallel load across workers
- **mem_free** graph: memory available per worker

---

## 7. What to Look For and How to Diagnose Issues

### 7.1 Is My Cache Being Used?

Run this after Cell 16 executes:

```python
# Check cache storage level for both core tables
print("axis_core storage level:", axis_core.storageLevel)
print("fin_core storage level: ", fin_core.storageLevel)

# Check how many partitions are cached
sc = spark.sparkContext
print(f"axis_core partitions: {axis_core.rdd.getNumPartitions()}")
print(f"fin_core partitions:  {fin_core.rdd.getNumPartitions()}")
```

In Spark UI → **Storage** tab:
- You should see `axis_core` and `fin_core` listed with their partition counts
- `Fraction Cached` should be close to 100%
- If not 100%, workers are spilling to disk → consider adding workers

### 7.2 Is My Join Being Broadcast?

```python
# Check the physical plan of a join
from pyspark.sql import functions as F
test_join = axis_core.join(fin_core, axis_core.axis_id == fin_core.fin_id, "inner")
test_join.explain(mode="formatted")
```

Look for `BroadcastHashJoin` in the output. If you see `SortMergeJoin` for a table you expected to be broadcast, the table may exceed `autoBroadcastJoinThreshold`.

### 7.3 Are Tasks Evenly Distributed? (Checking for Skew)

In Spark UI → Stages → click on a stage → look at **Task Duration** histogram:
- **Healthy:** bell curve, most tasks finish around the median
- **Skewed:** long tail — a few tasks take 10-100× longer than others

To detect skew programmatically:

```python
# Check cardinality of join keys — high counts = potential skew
axis_core.groupBy("SourceSystemTradeId") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .limit(20) \
    .show(truncate=False)

fin_core.groupBy("tradeid") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .limit(20) \
    .show(truncate=False)
```

### 7.4 How Many Partitions After a Shuffle?

```python
# After a join, check partition count
result = axis_core.join(fin_core, ["axis_id"], "inner")
print(f"Partitions after join: {result.rdd.getNumPartitions()}")
# Should be ~320 (spark.sql.shuffle.partitions)
# AQE may coalesce this lower if data is small
```

### 7.5 Memory Check — Are Workers Spilling?

In Spark UI → Stages → **Spill (Memory)** and **Spill (Disk)** columns:
- Any value > 0 in **Spill (Disk)** means workers ran out of RAM
- Fix: increase `spark.sql.shuffle.partitions` (more, smaller partitions) or add workers

---

## 8. Common Problems and Fixes

| Problem | Symptom | Fix |
|---------|---------|-----|
| **Data skew on join key** | 1 task takes 100× longer | AQE `skewJoin.enabled=true` (already set); or add salt to the key |
| **Cache miss** | `FileScan` in explain, slow rules after first one | Call `.count()` immediately after `.persist()` to materialise |
| **Too many shuffle partitions** | Thousands of tiny tasks | AQE coalesces automatically; or lower `shuffle.partitions` |
| **Too few shuffle partitions** | OOM / large spill | Increase `shuffle.partitions` (currently 320) |
| **Driver bottleneck** | CPU flat (0%) during loops | Rule loop is sequential by design; move repeated Python logic into Spark operations |
| **Broadcast threshold too low** | `SortMergeJoin` on small table | Increase `autoBroadcastJoinThreshold` or use `F.broadcast()` explicitly |
| **Workers idle between rules** | CPU drops to 0% between each rule | Expected — driver accumulates `matched_axis_ids`; checkpoint every 5 rules (already done) |
| **P15 explosion** | 1 rule produces 100M+ rows | `top_k=5`, `MAX_FIN_PER_KEY=50`, system equi-join (all already applied in v5.0) |

---

## Quick Reference: Databricks UI Navigation

```
Databricks Workspace
└── Compute
    └── [Your Cluster]
        ├── Metrics tab          → CPU, Memory, Network, Disk per worker (live)
        └── Spark UI button
            ├── Jobs tab         → One job per .count()/.show()/.write action
            │   └── [Job]
            │       └── Stages  → One stage per shuffle boundary
            │           └── [Stage]
            │               ├── Task Duration histogram  → check for skew
            │               ├── Shuffle Read/Write       → check for hot keys
            │               └── Spill (Memory/Disk)      → check for OOM
            ├── Storage tab      → Which DataFrames are cached, % cached
            ├── SQL/DataFrame tab → Physical plan per query
            │   └── [Query]
            │       └── Visual plan → look for BroadcastHashJoin, InMemoryTableScan
            └── Executors tab    → Per-worker stats (tasks, memory, GC time)
```

---

*Generated for Derivatives Matching Engine v5.1-pyspark — Databricks Runtime 16.4 LTS*  
*Cluster: 122 GB / 16 cores / 2–10 workers*
