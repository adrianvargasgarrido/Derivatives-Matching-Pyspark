# Derivatives Trade Matching Engine — PySpark

**Version:** v4.0-pyspark  
**Date:** 2026-02-23  
**Environment:** Apache Spark / Databricks  

---

## Overview

This repository contains a production-grade **derivatives trade matching engine** that reconciles positions between two financial systems:

| System | Role | Scale |
|---|---|---|
| **Axis** | Internal trade booking system | ~4 million rows, 100+ columns |
| **Finstore** | Regulatory capital reporting system | ~20 million rows, 100+ columns |

The engine implements **15 deterministic BRD matching rules** plus **2 greedy / probabilistic fallback strategies** to produce a fully auditable, 1-to-1 match set suitable for regulatory capital reporting.

---

## Repository Contents

| File | Description |
|---|---|
| `Derivatives_Matching_PySpark.ipynb` | Production PySpark notebook — the matching engine |
| `PySpark_Notebook_Technical_Guide.md` | Deep-dive technical documentation for the notebook |

---

## The Notebook — `Derivatives_Matching_PySpark.ipynb`

### What it does

The notebook takes raw Axis and Finstore trade data, applies a structured waterfall of matching rules, and produces:

- **`matched_all_combined`** — all matched trade pairs with full enriched columns and audit metadata
- **`unmatched_axis_final`** — Axis trades that could not be matched
- **`unmatched_finstore_final`** — Finstore trades that could not be matched
- **`matched_brd_layer`** — matches from deterministic BRD rules only
- **`matched_greedy_layer`** — matches from probabilistic greedy strategies
- **`dq_report`** — data quality validation results
- **`axis_unmatched_reasons` / `fin_unmatched_reasons`** — explainability breakdown of why each trade went unmatched

All outputs are written as **Delta tables** for ACID compliance, time-travel, and downstream queryability.

### Architecture

The notebook uses a **Candidates → Score → Resolve** architecture rather than the traditional iterative pool-removal waterfall:

```
All 15 BRD rules
      │
      ▼  build_candidates_for_rule() × 15
┌─────────────────────────────────────┐
│  Candidate edges                    │
│  (axis_id, fin_id, priority,        │
│   amount_diff, key_strength, …)     │
└─────────────────────────────────────┘
      │
      ▼  3-pass window ranking (priority ASC, key_strength DESC, amount_diff ASC)
┌─────────────────────────────────────┐
│  brd_matches  (1-to-1 guaranteed)   │
└─────────────────────────────────────┘
      │
      ▼  Anti-join
┌─────────────────────────────────────┐
│  Unmatched → Greedy Layer 2         │
└─────────────────────────────────────┘
      │
      ▼  Wide enrichment (single join at the end)
┌─────────────────────────────────────┐
│  matches_enriched (100+ cols)       │
└─────────────────────────────────────┘
```

This produces **identical results** to the original Pandas waterfall while being viable at 4M × 20M scale.

### Two Matching Layers

#### Layer 1 — BRD Deterministic (15 Rules)

| Category | Rules | Join Keys |
|---|---|---|
| **SOPHIS** (P1–P3) | 3 rules | `DerivedSophisId` ↔ `fissnumber` / `tradeid` |
| **OTC** (P4–P13) | 10 rules | `SourceSystemTradeId` / `DerivedSophisId` / `DerivedDelta1Id` ↔ `tradeid` / `alternatetradeid1` / `alternatetradeid2` |
| **ETD** (P14–P15) | 2 rules | `SourceSystemInstrumentId` ↔ `instrumentid` |

#### Layer 2 — Greedy / Probabilistic (2 Strategies)

| Strategy | Blocking Key | Tolerance | Use case |
|---|---|---|---|
| **Strategy 1** | Same counterparty | 1% amount difference | Trades with reliable counterparty IDs but no key match |
| **Strategy 2** | Amount bucket ±1 | 0.1% amount difference | Trades with no counterparty anchor |

### Key Design Decisions

| Decision | What and Why |
|---|---|
| **Core / Wide split** | All 15 BRD joins run on ~11-column "core" schemas. The full 100+ column wide schemas are joined back only once at the end — reducing shuffle I/O by up to 17×. |
| **No Python UDFs** | SOPHIS ID extraction, ReconSubProduct classification, and all derivations use native Spark SQL functions (`F.split`, `F.element_at`, `F.floor`, etc.), executing inside the JVM with no Python serialisation overhead. |
| **3-pass window ranking** | Enforces 1-to-1 matching globally across all 15 rules in a single shuffle, replacing 15 sequential join-and-remove cycles. |
| **Greedy blocking** | Strategy 1 blocks on counterparty (equi-join), Strategy 2 blocks on amount bucket ±1 expansion — prevents O(n²) cross-joins. |
| **AQE + skew join** | Adaptive Query Execution rewrites join plans at runtime; skew join handling splits hot-key partitions that would otherwise bottleneck a single task. |
| **Delta output** | ACID transactions, time-travel (rollback to any previous version), schema enforcement, and ZORDER optimisation for downstream query performance. |
| **Auditability columns** | Every matched row carries `run_id`, `batch_id`, `rule_version`, `match_timestamp` — enabling full traceability for regulatory audit. |
| **Explainability** | Every unmatched trade is classified as either `no_candidate_key_found` or `candidate_existed_but_consumed_by_higher_priority`, giving operations teams an immediately actionable breakdown. |

### Notebook Sections

| Section | What it does |
|---|---|
| 1 | Spark session + AQE configuration |
| 2 | BRD constants, system classifications, lineage config |
| 3 | All 15 BRD matching rules as dictionaries |
| 4 | Bronze layer ingestion + metadata stamps |
| 5 | Scope exclusion (SOPHIS / DELTA1) |
| 6 | Silver layer derivations — `DerivedSophisId`, `DerivedDelta1Id`, `ReconSubProduct` |
| 7 | Core / Wide split + cache core tables |
| 8 | `build_candidates_for_rule()` function definition |
| 9 | Generate all 15 BRD candidate edge sets + union |
| 10 | `resolve_one_to_one()` — 3-pass window ranking |
| 11 | Compute unmatched pools via anti-join |
| 12 | Greedy Strategy 1 — amount + counterparty (1%) |
| 13 | Greedy Strategy 2 — amount bucket blocking (0.1%) |
| 14 | Greedy layer summary |
| 15 | Final consolidation — union all matches |
| 16 | Enrichment — join back wide columns (single join) |
| 17 | Matches by source system breakdown |
| 18 | Remaining unmatched by source system |
| 18b | Data quality validation |
| 18c | Explainability — unmatched reason breakdown |
| 19 | Save results as Delta + optional CSV export |
| 20 | Summary report |
| 21 | Cleanup — unpersist all cached DataFrames |

---

## The Technical Guide — `PySpark_Notebook_Technical_Guide.md`

A 987-line deep-dive reference covering:

- **Why the Pandas POC cannot be directly ported at scale** — four concrete failure modes (repeated wide shuffles, 13 anti-joins, 100+ column payload, skew explosions) and the specific architectural choices that solve each
- **Section-by-section walkthrough** — for every notebook section: what the code does, why each specific implementation choice was made, and what the performance implications are
- **All 15 BRD rules in detail** — for each rule: the join keys, system filter logic, key strength, rationale for its priority position in the waterfall, and a worked example
- **Greedy strategies in detail** — the blocking mechanism, tolerance rationale, and deduplication logic
- **End-to-end data flow diagram** — full ASCII diagram showing every transformation from raw CSV to Delta output
- **Key efficiency decisions table** — side-by-side comparison of every major design choice between the Pandas POC and the PySpark production notebook

### Quick Reference — Technical Guide Contents

| Section | Pages |
|---|---|
| Overview & Architecture | §1 |
| Why not port Pandas directly? | §2 |
| Section-by-section walkthrough (all 21 sections) | §3 |
| All 15 BRD rules with rationale and examples | §4 |
| Greedy strategy mechanics | §5 |
| End-to-end data flow diagram | §6 |
| Efficiency decisions summary table | §7 |

---

## Running the Notebook

### Prerequisites

- Apache Spark 3.3+ (Databricks Runtime 12+ recommended)
- Delta Lake enabled on the cluster
- Input files accessible via the configured `INPUT_DIR` path

### Configuration (Cell 2)

```python
# File paths — adjust for your ADLS mount / DBFS path
INPUT_DIR          = "/mnt/data/derivatives"
INPUT_FILE_AXIS    = f"{INPUT_DIR}/axis_sample_poc.csv"
INPUT_FILE_FINSTORE = f"{INPUT_DIR}/finstore_sample_poc.csv"
OUTPUT_DIR         = f"{INPUT_DIR}/matching_results"

# Scope
EXCLUDE_SOPHIS_DELTA1 = True   # recommended for standard runs
```

### Local Testing

Uncomment the SparkSession builder block in Section 1 and point the input paths at local files. A small test dataset (`axis_sample_poc.csv`, `finstore_sample_poc.csv`) covering all 15 rules and both greedy strategies is available in the project directory.

### Expected outputs (test dataset)

With `EXCLUDE_SOPHIS_DELTA1 = True`:

| Layer | Expected matches |
|---|---|
| P1 SOPHIS `DerivedSophisId ↔ fissnumber` | 1 |
| P2 SOPHIS composite | 1 |
| P3 SOPHIS `DerivedSophisId ↔ tradeid` | 1 |
| P4 OTC `SourceSystemTradeId ↔ tradeid` | 2 |
| P6 OTC `SourceSystemTradeId ↔ alternatetradeid1` | 1 |
| P8 OTC `DerivedSophisId ↔ fissnumber` (SOPHISFX) | 1 |
| P12 OTC `SourceSystemTradeId ↔ alternatetradeid2` | 1 |
| P15 ETD `SourceSystemInstrumentId ↔ instrumentid` | 2 |
| Greedy S1 (amount + counterparty 1%) | 1 |
| Greedy S2 (amount 0.1%) | 1 |
| **Unmatched Axis** | **1** |
| **Unmatched Finstore** | **1** |

---

## Scope Note

SOPHIS and DELTA1 systems are **excluded by default** (`EXCLUDE_SOPHIS_DELTA1 = True`). Their trade identifiers do not exist in Finstore — this is a data mapping gap at source system level, not a matching algorithm issue. Including them would inflate the unmatched count with guaranteed failures and obscure genuine reconciliation breaks. Set to `False` to include them (activates P11 `DerivedDelta1Id ↔ tradeid`).

---

## Best Practices Implemented

This notebook follows all 9 sections of the internal `best_practices.md` architecture guide:

| Practice | Implementation |
|---|---|
| §1 Guiding Principles | Candidates → Score → Resolve; no iterative pool removal; block aggressively |
| §2A Bronze layer | `_ingest_timestamp`, `_source_file`, `_batch_id` on every raw row |
| §2B Silver layer | Core/Wide split; derived keys precomputed before any join |
| §2C Candidate generation | Per-rule function; single union; no wide shuffles |
| §2D Scoring | `amount_diff`, `amount_rel_diff`, `key_strength` on every candidate |
| §2E Resolution | 3-pass window ranking with stable tiebreakers |
| §3 Waterfall | Priority-ordered window ranking replaces iterative pool mutation |
| §4 Blocking | Counterparty equi-join (S1) + amount bucket ±1 expansion (S2) |
| §5 Performance | AQE + skew join; core-only matching; ZORDER guidance on Delta writes |
| §6 Auditability | `run_id`, `batch_id`, `rule_version`, `match_timestamp` on every match row |
| §6 Explainability | `unmatched_reason` on every unmatched trade |
| §7 Data quality | `validate_dataframe()` null/range checks on inputs and outputs |
| §9 Failure modes | Architecture prevents Cartesian explosion, repeated wide shuffles, OOM, skew |
