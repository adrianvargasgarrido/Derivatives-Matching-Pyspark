1. Purpose

This project implements a two-layer trade matching engine to reconcile derivatives trades between:
	•	Axis (Front Office / Risk System) → Parent trade records
	•	Finstore (Back Office / Finance System) → Individual trade leg records

The system uses:
	1.	Deterministic BRD rules (Layer 1)
	2.	Greedy probabilistic matching (Layer 2)

⸻

2. High-Level Architecture


INPUT DATA
  ├── Axis Sample (~90k)
  └── Finstore Sample (~96k)

↓ Scope Exclusion (optional)

↓ Pre-Reconciliation Derivations

↓ Layer 1: Deterministic BRD Matching (15 rules, waterfall)

↓ Layer 2: Greedy Matching
      ├── Strategy 1: Amount + Counterparty (1%)
      └── Strategy 2: Strict Amount (0.1%)

↓ Final Consolidation

↓ Output Files

3. Configuration

3.1 Scope


EXCLUDE_SOPHIS_DELTA1 = True

f enabled → remove systems:
	•	SOPHIS-LONDON
	•	SOPHIS-NEWYORK
	•	SOPHIS-TOKYO
	•	SOPHISFX-LONDON
	•	DELTA1-LONDON
	•	DELTA1-NEWYORK

⸻

3.2 Greedy Parameters

GREEDY_TOLERANCE_PCT = 0.01     # 1%
STRICT_TOLERANCE_PCT = 0.001   # 0.1%

4. Input Data Requirements

Axis Required Columns
	•	SourceSystemName
	•	SourceSystemTradeId
	•	BookId
	•	CounterpartyId
	•	SACCMTM
	•	SourceSystemInstrumentId (ETD only)

Finstore Required Columns
	•	tradeid
	•	alternatetradeid1
	•	alternatetradeid2
	•	masterbookid
	•	tradingsystembook
	•	instrumentid
	•	counterpartyid
	•	gbpequivalentamount

⸻

5. Pre-Reconciliation Derivations

Before matching, create:

5.1 Tracking Columns
	•	OriginalAxisIndex
	•	OriginalFinstoreIndex

5.2 Derived Fields

DerivedSophisId

Extract third part of trade ID:
10250181-40624783-N601844NE-EQT00063116
→ N601844NE

def extract_third_part(trade_id: str) -> str:
    if pd.isna(trade_id):
        return None
    parts = str(trade_id).split("-")
    return parts[2] if len(parts) >= 3 else None

DerivedDelta1Id

Same extraction logic for DELTA1.

⸻

ReconSubProduct

Classify as:
	•	OTC
	•	ETD
	•	OTC-Default

⸻

6. Layer 1 – Deterministic Matching (BRD Rules)

Core Concept
	•	15 rules
	•	Strict waterfall execution
	•	After each rule:
	•	Remove matched records from both pools
	•	Prevent double-matching

⸻

6.1 Execution Model

for rule in ordered_rules:
    matched = execute_matching_rule(axis_remaining, finstore_remaining, rule)

    axis_remaining = axis_remaining.drop(matched.axis_indices)
    finstore_remaining = finstore_remaining.drop(matched.finstore_indices)

    all_matches.append(matched)

6.2 Rule Categories

Category
Rules
Priority
SOPHIS
3
1–3
OTC
10
4–13
ETD
2
14–15


6.3 Primary Deterministic Rule

Rule 4 (Most Important)

Axis.SourceSystemTradeId == Finstore.tradeid


This typically generates ~65% of all matches.

⸻

6.4 Composite Key Matching

Some rules require multi-column joins.

Example:

Axis(SourceSystemTradeId + BookId)
=
Finstore(tradeid + masterbookid)


def create_composite_key(df, columns):
    return df[columns].astype(str).agg("_".join, axis=1)

7. Layer 2 – Greedy Matching

Applied only to remaining unmatched records.

⸻

7.1 Strategy 1 – Amount + Counterparty

Logic

For each counterparty:
	1.	Filter Finstore records with same counterparty
	2.	Apply 1% tolerance:
    3.	Select candidate with smallest difference

7.2 Strategy 2 – Strict Amount Only

Used after Strategy 1.

Blocking by Amount Buckets


8. Waterfall Principle (CRITICAL)

After each rule:
	•	Remove matched Axis rows
	•	Remove matched Finstore rows
	•	Never allow reuse

Prevents:
	•	Double counting
	•	One-to-many accidental matches
	•	Rule priority conflicts

⸻

9. Execution Flow
	1.	Load Data
	2.	Scope Exclusion
	3.	Pre-Reconciliation Derivations
	4.	Execute SOPHIS Rules (1–3)
	5.	Execute OTC Rules (4–13)
	6.	Execute ETD Rules (14–15)
	7.	Consolidate Layer 1
	8.	Execute Greedy Strategy 1
	9.	Execute Greedy Strategy 2
	10.	Consolidate Layer 2
	11.	Combine Final Matches
	12.	Save Outputs

⸻

10. Outputs

Saved to:

WORKING_DIR/greedy_results/

Files
	•	matched_brd_layer.csv
	•	matched_greedy_layer.csv
	•	matched_all_combined.csv
	•	unmatched_axis_final.csv
	•	matching_summary_report.txt

⸻

11. Scaling to Production (IMPORTANT)

Current POC uses Pandas.

For 4M × 20M production:

Must Use:
	•	PySpark
	•	Partitioned joins
	•	Broadcast joins where possible
	•	Avoid nested loops
	•	Avoid full cross joins

Replace:
	•	groupby loops → Spark window functions
	•	isin() → broadcast hash joins
	•	Pandas merge → Spark join with partitioning


3. Design Principles
	•	Deterministic rules take priority
	•	Strict waterfall removal
	•	Conservative greedy tolerance
	•	Blocking to reduce search space
	•	Clear separation of layers

⸻

14. Core Engine Functions Required

To reimplement cleanly:
	•	load_data()
	•	apply_scope_exclusion()
	•	derive_fields()
	•	execute_matching_rule()
	•	run_layer1()
	•	run_greedy_strategy1()
	•	run_greedy_strategy2()
	•	consolidate_results()
	•	save_outputs()



