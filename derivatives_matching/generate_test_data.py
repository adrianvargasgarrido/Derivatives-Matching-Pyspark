"""
generate_test_data.py
=====================
Generates small, deterministic CSV test datasets for:
  - axis_sample_poc.csv       (Axis trades — shared by both notebooks)
  - finstore_sample_poc.csv   (Finstore trades — PySpark / lowercase headers)
  - finstore_pandas_poc.csv   (Finstore trades — Pandas / PascalCase headers)
  - sds_book_mapping.csv      (SDS book mapping — optional)

Column name conventions:
  PySpark notebook  → lowercase finstore columns  (fissnumber, tradeid, …)
  Pandas  notebook  → PascalCase finstore columns (FissNumber, TradeId, …)

Both share the same Axis CSV (PascalCase Axis headers are identical in both).

Coverage per BRD rule:
  P1  SOPHIS:  DerivedSophisId <> fissnumber / FissNumber
  P2  SOPHIS:  DerivedSophisId + BookId <> fissnumber + tradingsystembook / TradingSystem
  P3  SOPHIS:  DerivedSophisId <> tradeid / TradeId
  P4  OTC:     SourceSystemTradeId <> tradeid / TradeId
  P6  OTC:     SourceSystemTradeId <> alternatetradeid1 / AlternativeTradeId
  P8  OTC:     DerivedSophisId <> fissnumber / FissNumber  (SOPHISFX system)
  P11 OTC:     DerivedDelta1Id <> tradeid / TradeId
  P12 OTC:     SourceSystemTradeId <> alternatetradeid2 / AlternativeTradeId2
  P15 ETD:     SourceSystemInstrumentId <> instrumentid / InstrumentId
  Greedy S1:   Amount + Counterparty (1% tol)
  Greedy S2:   Amount only (0.1% tol, no counterparty)

Usage:
  python generate_test_data.py
  # → writes CSV files to the current directory
"""

import csv
import os
import random

random.seed(42)

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Written {len(rows):>3} rows → {os.path.basename(path)}")


def axis_row(**kwargs):
    """Return a full Axis row with sensible defaults."""
    defaults = dict(
        SourceSystemName="IFLOW-EUROPE",
        SourceSystemTradeId="",
        BookId="BOOK-DEFAULT",
        CounterpartyId="CPTY-001",
        SACCRMTM=1_000_000.0,
        SourceSystemInstrumentId="",
        # Extra wide columns (simulate 100+ real columns)
        TradeDate="2025-01-15",
        MaturityDate="2026-01-15",
        Currency="GBP",
        Product="IRS",
        Desk="RATES",
        Trader="TRADER1",
        LegalEntity="BARCLAYS PLC",
        Region="EMEA",
        AssetClass="RATES",
    )
    defaults.update(kwargs)
    return defaults


def fin_row(**kwargs):
    """Return a full Finstore row with sensible defaults."""
    defaults = dict(
        tradeid="",
        alternatetradeid1="",
        alternatetradeid2="",
        masterbookid="",
        tradingsystembook="",
        fissnumber="",
        instrumentid="",
        counterpartyid="CPTY-001",
        gbpequivalentamount=1_000_000.0,
        # Extra wide columns
        tradedate="2025-01-15",
        maturitydate="2026-01-15",
        currency="GBP",
        product="IRS",
        status="ACTIVE",
        region="EMEA",
        bookedsystem="FINSTORE",
    )
    defaults.update(kwargs)
    return fin_row_clean(defaults)


def fin_row_clean(d):
    return d


# ---------------------------------------------------------------------------
# Axis column list (same order as the real CSVs)
# ---------------------------------------------------------------------------
AXIS_COLS = [
    "SourceSystemName", "SourceSystemTradeId", "BookId", "CounterpartyId",
    "SACCRMTM", "SourceSystemInstrumentId",
    "TradeDate", "MaturityDate", "Currency", "Product", "Desk", "Trader",
    "LegalEntity", "Region", "AssetClass",
]

FIN_COLS = [
    "tradeid", "alternatetradeid1", "alternatetradeid2", "masterbookid",
    "tradingsystembook", "fissnumber", "instrumentid", "counterpartyid",
    "gbpequivalentamount",
    "tradedate", "maturitydate", "currency", "product", "status", "region",
    "bookedsystem",
]

# PascalCase equivalents for Pandas notebook
# Note: Pandas notebook uses a single "AlternativeTradeId" column (covers both alt1 and alt2)
FIN_COLS_PANDAS = [
    "TradeId", "AlternativeTradeId", "MasterbookId",
    "TradingSystem", "FissNumber", "InstrumentId", "CounterpartyId",
    "gbpequivalentamount",
    "tradedate", "maturitydate", "currency", "product", "status", "region",
    "bookedsystem",
]

# Mapping from lowercase → PascalCase for Finstore
# alternatetradeid1 is used as the Pandas AlternativeTradeId (alt1 takes priority)
FIN_COL_MAP_PANDAS = {
    "tradeid":              "TradeId",
    "alternatetradeid1":    "AlternativeTradeId",
    "masterbookid":         "MasterbookId",
    "tradingsystembook":    "TradingSystem",
    "fissnumber":           "FissNumber",
    "instrumentid":         "InstrumentId",
    "counterpartyid":       "CounterpartyId",
    "gbpequivalentamount":  "gbpequivalentamount",
    "tradedate":            "tradedate",
    "maturitydate":         "maturitydate",
    "currency":             "currency",
    "product":              "product",
    "status":               "status",
    "region":               "region",
    "bookedsystem":         "bookedsystem",
}

# ---------------------------------------------------------------------------
# Build datasets
# ---------------------------------------------------------------------------

axis_rows = []
fin_rows = []

# ===========================================================================
# SOPHIS trades (DerivedSophisId is extracted from 3rd hyphen-segment)
# Format: PREFIX-SYSTEM-{sophisId}
# ===========================================================================

# P1 — SOPHIS: DerivedSophisId <> fissnumber
# axis DerivedSophisId = "S001" → fin fissnumber = "S001"
axis_rows.append(axis_row(
    SourceSystemName="SOPHIS-LONDON",
    SourceSystemTradeId="TRD-LON-S001",
    BookId="BOOK-SOPHIS-1",
    CounterpartyId="CPTY-SOF-1",
    SACCRMTM=500_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-99001",
    fissnumber="S001",
    counterpartyid="CPTY-SOF-1",
    gbpequivalentamount=500_000.0,
))

# P2 — SOPHIS: DerivedSophisId + BookId <> fissnumber + tradingsystembook
# Same sophisId but different BookId to avoid P1 match (P2 = more specific)
axis_rows.append(axis_row(
    SourceSystemName="SOPHIS-NEWYORK",
    SourceSystemTradeId="TRD-NY-S002",
    BookId="BOOK-SOPHIS-2",
    CounterpartyId="CPTY-SOF-2",
    SACCRMTM=750_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-99002",
    fissnumber="S002",
    tradingsystembook="BOOK-SOPHIS-2",
    counterpartyid="CPTY-SOF-2",
    gbpequivalentamount=750_000.0,
))

# P3 — SOPHIS: DerivedSophisId <> tradeid
axis_rows.append(axis_row(
    SourceSystemName="SOPHIS-TOKYO",
    SourceSystemTradeId="TRD-TKY-S003",
    BookId="BOOK-SOPHIS-3",
    CounterpartyId="CPTY-SOF-3",
    SACCRMTM=250_000.0,
))
fin_rows.append(fin_row(
    tradeid="S003",
    fissnumber="",
    counterpartyid="CPTY-SOF-3",
    gbpequivalentamount=250_000.0,
))

# ===========================================================================
# OTC trades
# ===========================================================================

# P4 — SourceSystemTradeId <> tradeid
axis_rows.append(axis_row(
    SourceSystemName="IFLOW-EUROPE",
    SourceSystemTradeId="OTC-TRADE-0001",
    BookId="BOOK-OTC-1",
    CounterpartyId="CPTY-OTC-1",
    SACCRMTM=1_200_000.0,
))
fin_rows.append(fin_row(
    tradeid="OTC-TRADE-0001",
    counterpartyid="CPTY-OTC-1",
    gbpequivalentamount=1_200_000.0,
))

# P6 — SourceSystemTradeId <> alternatetradeid1
# (tradeid intentionally differs so P4 won't fire)
axis_rows.append(axis_row(
    SourceSystemName="SUMMIT-LONDON",
    SourceSystemTradeId="SUM-TRADE-0002",
    BookId="BOOK-OTC-2",
    CounterpartyId="CPTY-OTC-2",
    SACCRMTM=800_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-ALT-001",           # different — P4 won't match
    alternatetradeid1="SUM-TRADE-0002",
    counterpartyid="CPTY-OTC-2",
    gbpequivalentamount=800_000.0,
))

# P8 — DerivedSophisId <> fissnumber  (non-SOPHIS system, DerivedSophisId computed)
# NOTE: DerivedSophisId derivation only fires for SOPHIS systems in Silver layer.
# For a non-SOPHIS system to match via P8, it would need DerivedSophisId pre-populated.
# In the test we use a SOPHISFX trade (which IS in SOPHIS_SYSTEMS but excluded from
# SOPHIS-specific rule P1 since P8 is an OTC rule with no system filter).
# We use SOPHISFX-LONDON which is in SOPHIS_SYSTEMS so DerivedSophisId is computed.
axis_rows.append(axis_row(
    SourceSystemName="SOPHISFX-LONDON",
    SourceSystemTradeId="FX-LON-S004",
    BookId="BOOK-FX-1",
    CounterpartyId="CPTY-FX-1",
    SACCRMTM=2_000_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-99003",
    fissnumber="S004",
    counterpartyid="CPTY-FX-1",
    gbpequivalentamount=2_000_000.0,
))

# P11 — DerivedDelta1Id <> tradeid
# DerivedDelta1Id extracted from 3rd hyphen segment for DELTA1 systems
# DELTA1 trades are normally excluded (EXCLUDE_SOPHIS_DELTA1=True).
# When scope includes DELTA1 (set flag to False), this rule fires.
# We include these rows so the rule is exercised when exclusion is off.
axis_rows.append(axis_row(
    SourceSystemName="DELTA1-LONDON",
    SourceSystemTradeId="D1L-REF-D001",
    BookId="BOOK-D1-1",
    CounterpartyId="CPTY-D1-1",
    SACCRMTM=3_000_000.0,
))
fin_rows.append(fin_row(
    tradeid="D001",
    counterpartyid="CPTY-D1-1",
    gbpequivalentamount=3_000_000.0,
))

# P12 — SourceSystemTradeId <> alternatetradeid2
axis_rows.append(axis_row(
    SourceSystemName="GCD-NEWYORK",
    SourceSystemTradeId="GCD-TRADE-0005",
    BookId="BOOK-GCD-1",
    CounterpartyId="CPTY-GCD-1",
    SACCRMTM=1_500_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-GCD-ALT",
    alternatetradeid1="",
    alternatetradeid2="GCD-TRADE-0005",
    counterpartyid="CPTY-GCD-1",
    gbpequivalentamount=1_500_000.0,
))

# P4 again — a second clean OTC match to pad volume
axis_rows.append(axis_row(
    SourceSystemName="OPENLINK-LONDON",
    SourceSystemTradeId="OPN-TRADE-0006",
    BookId="BOOK-OPN-1",
    CounterpartyId="CPTY-OPN-1",
    SACCRMTM=600_000.0,
))
fin_rows.append(fin_row(
    tradeid="OPN-TRADE-0006",
    counterpartyid="CPTY-OPN-1",
    gbpequivalentamount=600_000.0,
))

# ===========================================================================
# ETD trades
# ===========================================================================

# P15 — SourceSystemInstrumentId <> instrumentid
axis_rows.append(axis_row(
    SourceSystemName="ODH-GMI-LONDON",
    SourceSystemTradeId="ETD-TRADE-7001",
    SourceSystemInstrumentId="INST-XYZ-001",
    BookId="BOOK-ETD-1",
    CounterpartyId="CPTY-ETD-1",
    SACCRMTM=400_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-ETD-7001",
    instrumentid="INST-XYZ-001",
    counterpartyid="CPTY-ETD-1",
    gbpequivalentamount=400_000.0,
))

# P15 — second ETD match
axis_rows.append(axis_row(
    SourceSystemName="ODH-GMI-NEWYORK",
    SourceSystemTradeId="ETD-TRADE-7002",
    SourceSystemInstrumentId="INST-ABC-002",
    BookId="BOOK-ETD-2",
    CounterpartyId="CPTY-ETD-2",
    SACCRMTM=900_000.0,
))
fin_rows.append(fin_row(
    tradeid="FIN-ETD-7002",
    instrumentid="INST-ABC-002",
    counterpartyid="CPTY-ETD-2",
    gbpequivalentamount=900_000.0,
))

# ===========================================================================
# GREEDY Strategy 1 — Amount + Counterparty (1% tolerance)
# No BRD key match; amounts within 1% of each other, same counterparty.
# ===========================================================================

axis_rows.append(axis_row(
    SourceSystemName="IFLOW-AMERICAS",
    SourceSystemTradeId="GRD1-AXIS-001",
    BookId="BOOK-GRD-1",
    CounterpartyId="CPTY-GRD-1",
    SACCRMTM=1_000_000.0,
))
fin_rows.append(fin_row(
    tradeid="GRD1-FIN-001",           # key doesn't match Axis trade ID
    counterpartyid="CPTY-GRD-1",
    gbpequivalentamount=1_005_000.0,  # 0.5% diff — within 1% tolerance
))

# ===========================================================================
# GREEDY Strategy 2 — Amount only (0.1% tolerance, no counterparty match)
# ===========================================================================

axis_rows.append(axis_row(
    SourceSystemName="FISS-LONDON",
    SourceSystemTradeId="GRD2-AXIS-001",
    BookId="BOOK-GRD-2",
    CounterpartyId="CPTY-GRD-2A",
    SACCRMTM=5_000_000.0,
))
fin_rows.append(fin_row(
    tradeid="GRD2-FIN-001",
    counterpartyid="CPTY-GRD-2B",    # different counterparty → greedy S1 won't fire
    gbpequivalentamount=5_001_000.0,  # 0.02% diff — within 0.1% strict tolerance
))

# ===========================================================================
# INTENTIONALLY UNMATCHED — exercise unmatched-reason tables
# ===========================================================================

# No key or amount match anywhere → reason: "no_candidate_key_found"
axis_rows.append(axis_row(
    SourceSystemName="IFLOW-EUROPE",
    SourceSystemTradeId="NOMATCH-AX-001",
    BookId="BOOK-NM-1",
    CounterpartyId="CPTY-NOBODY",
    SACCRMTM=99_999_999.0,            # huge amount, no Finstore counterpart
))

# Finstore trade with no Axis counterpart
fin_rows.append(fin_row(
    tradeid="NOMATCH-FIN-001",
    counterpartyid="CPTY-NOBODY-FIN",
    gbpequivalentamount=88_888_888.0,
))

# ===========================================================================
# Write CSVs
# ===========================================================================

axis_path = os.path.join(OUT_DIR, "axis_sample_poc.csv")
fin_path  = os.path.join(OUT_DIR, "finstore_sample_poc.csv")          # PySpark (lowercase)
fin_pandas_path = os.path.join(OUT_DIR, "finstore_pandas_poc.csv")    # Pandas  (PascalCase)

write_csv(axis_path, axis_rows, AXIS_COLS)
write_csv(fin_path,  fin_rows,  FIN_COLS)

# Produce PascalCase version for Pandas notebook
# Also put the GCD alt-2 match into alternatetradeid1 so it is visible as
# AlternativeTradeId in the Pandas CSV (Pandas has only one alt-id column).
# The PySpark notebook uses alternatetradeid2 for P12 — so we keep alt2 for PySpark
# and copy the same value into alt1 for the Pandas CSV version below.

# Produce PascalCase version for Pandas notebook
# Drop alternatetradeid2 (Pandas has no such column) — copy its value into alt1 when alt1 is empty
def to_pandas_fin_row(r):
    alt1 = r["alternatetradeid1"] or r["alternatetradeid2"]  # merge alt2 into alt1
    return {
        "TradeId":              r["tradeid"],
        "AlternativeTradeId":   alt1,
        "MasterbookId":         r["masterbookid"],
        "TradingSystem":        r["tradingsystembook"],
        "FissNumber":           r["fissnumber"],
        "InstrumentId":         r["instrumentid"],
        "CounterpartyId":       r["counterpartyid"],
        "gbpequivalentamount":  r["gbpequivalentamount"],
        "tradedate":            r["tradedate"],
        "maturitydate":         r["maturitydate"],
        "currency":             r["currency"],
        "product":              r["product"],
        "status":               r["status"],
        "region":               r["region"],
        "bookedsystem":         r["bookedsystem"],
    }

fin_rows_pandas = [to_pandas_fin_row(r) for r in fin_rows]
write_csv(fin_pandas_path, fin_rows_pandas, FIN_COLS_PANDAS)

# ---------------------------------------------------------------------------
# SDS mapping (needed if DerivedMasterbookId rules are tested)
# Maps SourceSystemTradeId → masterbookid
# ---------------------------------------------------------------------------
SDS_COLS = ["SourceSystemTradeId", "MasterbookId", "BookId"]
sds_rows = [
    {"SourceSystemTradeId": "OTC-TRADE-0001", "MasterbookId": "MB-001", "BookId": "BOOK-OTC-1"},
    {"SourceSystemTradeId": "SUM-TRADE-0002", "MasterbookId": "MB-002", "BookId": "BOOK-OTC-2"},
    {"SourceSystemTradeId": "ETD-TRADE-7001", "MasterbookId": "MB-ETD-1", "BookId": "BOOK-ETD-1"},
]
sds_path = os.path.join(OUT_DIR, "sds_book_mapping.csv")
write_csv(sds_path, sds_rows, SDS_COLS)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print()
print("=" * 60)
print("TEST DATA SUMMARY")
print("=" * 60)
print(f"Axis rows                  : {len(axis_rows)}")
print(f"Finstore rows (PySpark CSV): {len(fin_rows)}")
print(f"Finstore rows (Pandas CSV) : {len(fin_rows_pandas)}")
print()
print("Expected matches by rule (with EXCLUDE_SOPHIS_DELTA1=True):")
print("  P1  SOPHIS DerivedSophisId <> FissNumber           : 1")
print("  P2  SOPHIS DerivedSophisId+BookId <> Fiss+TradSys  : 1")
print("  P3  SOPHIS DerivedSophisId <> TradeId              : 1")
print("  P4  OTC    SourceSystemTradeId <> TradeId          : 2")
print("  P6  OTC    SourceSystemTradeId <> AlternateTradeId1: 1")
print("  P8  OTC    DerivedSophisId <> FissNumber (SOPHISFX): 1")
print("  P11 OTC    DerivedDelta1Id <> TradeId              : 0  (DELTA1 excluded)")
print("  P12 OTC    SourceSystemTradeId <> AlternateTradeId2: 1")
print("  P15 ETD    SourceSystemInstrumentId <> InstrumentId: 2")
print("  Greedy S1  Amount+Counterparty 1%                  : 1")
print("  Greedy S2  Amount 0.1%                             : 1")
print()
print("Expected unmatched Axis : 1 (NOMATCH-AX-001)")
print("Expected unmatched Fin  : 1 (NOMATCH-FIN-001)")
print()
print("Files written to:", OUT_DIR)
print()
print("Usage:")
print("  PySpark notebook → INPUT_FILE_FINSTORE = .../finstore_sample_poc.csv")
print("  Pandas  notebook → INPUT_FILE_FINSTORE = .../finstore_pandas_poc.csv")
