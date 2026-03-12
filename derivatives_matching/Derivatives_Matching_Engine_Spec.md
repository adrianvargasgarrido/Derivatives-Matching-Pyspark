# Derivatives Trade Matching Engine -- Technical Specification

## 1. Purpose

This project implements a two-layer trade matching engine to reconcile
derivatives trades between:

-   Axis (Front Office / Risk System)
-   Finstore (Back Office / Finance System)

The system uses: 1. Deterministic BRD rules (Layer 1) 2. Greedy
probabilistic matching (Layer 2)

------------------------------------------------------------------------

## 2. High-Level Architecture

INPUT DATA - Axis - Finstore

↓ Scope Exclusion (optional)

↓ Pre-Reconciliation Derivations

↓ Layer 1: Deterministic BRD Matching (15 rules, waterfall)

↓ Layer 2: Greedy Matching - Strategy 1: Amount + Counterparty (1%) -
Strategy 2: Strict Amount (0.1%)

↓ Final Consolidation

↓ Output Files

------------------------------------------------------------------------

## 3. Configuration

### Scope Exclusion

EXCLUDE_SOPHIS_DELTA1 = True

If enabled, removes selected SOPHIS and DELTA1 systems before matching.

### Greedy Parameters

GREEDY_TOLERANCE_PCT = 0.01 STRICT_TOLERANCE_PCT = 0.001

------------------------------------------------------------------------

## 4. Input Data Requirements

### Axis Required Columns

-   SourceSystemName
-   SourceSystemTradeId
-   BookId
-   CounterpartyId
-   SACCMTM
-   SourceSystemInstrumentId (ETD only)

### Finstore Required Columns

-   tradeid
-   alternatetradeid1
-   alternatetradeid2
-   masterbookid
-   tradingsystembook
-   instrumentid
-   counterpartyid
-   gbpequivalentamount

------------------------------------------------------------------------

## 5. Pre-Reconciliation Derivations

### Tracking Columns

-   OriginalAxisIndex
-   OriginalFinstoreIndex

### DerivedSophisId Extraction

Example: 10250181-40624783-N601844NE-EQT00063116 → N601844NE

Function logic: Split by "-" and extract the third component if present.

------------------------------------------------------------------------

## 6. Layer 1 -- Deterministic Matching

-   15 ordered rules
-   Strict waterfall execution
-   After each rule:
    -   Remove matched Axis records
    -   Remove matched Finstore records

Primary rule: Axis.SourceSystemTradeId == Finstore.tradeid

Composite key rules combine multiple columns using underscore-separated
keys.

------------------------------------------------------------------------

## 7. Layer 2 -- Greedy Matching

### Strategy 1: Amount + Counterparty (1%)

For same counterparty: abs(axis_amount - finstore_amount) \<= 1% of
axis_amount

Select candidate with smallest difference.

### Strategy 2: Strict Amount Only (0.1%)

-   Use amount buckets (e.g., 1000 range)
-   Search nearby buckets
-   Apply 0.1% tolerance
-   Select smallest difference

------------------------------------------------------------------------

## 8. Waterfall Principle

After each rule execution: - Remove matched records from both pools -
Prevent double counting - Enforce rule priority

------------------------------------------------------------------------

## 9. Execution Flow

1.  Load Data
2.  Scope Exclusion
3.  Pre-Reconciliation Derivations
4.  Execute Layer 1 Rules (1--15)
5.  Execute Greedy Strategy 1
6.  Execute Greedy Strategy 2
7.  Consolidate Results
8.  Save Outputs

------------------------------------------------------------------------

## 10. Output Files

Saved under working directory:

-   matched_brd_layer.csv
-   matched_greedy_layer.csv
-   matched_all_combined.csv
-   unmatched_axis_final.csv
-   matching_summary_report.txt

------------------------------------------------------------------------

## 11. Production Scaling Notes

POC uses Pandas.

For production (4M × 20M records): - Use PySpark - Use partitioned
joins - Avoid nested loops - Avoid full cross joins - Use broadcast
joins when appropriate

------------------------------------------------------------------------

End of Document
