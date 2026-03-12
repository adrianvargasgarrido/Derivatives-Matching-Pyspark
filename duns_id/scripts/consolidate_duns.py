# =============================================================================
# consolidate_duns.py
# -----------------------------------------------------------------------------
# Reads all JSON files containing "DUNS_LTS" in their filename from
# data/reference/, consolidates them into a single unified DUNS ID list.
#
# Logic:
#   - Append all matching JSON files into one DataFrame
#   - Deduplicate on CS (unique customer identifier)
#   - For each unique CS: collect ALL duns values (as a list)
#   - For BU, IDS, SDS: keep the first non-null value
#
# Output:
#   data/output/unified_duns_list.csv       — one row per unique CS
#   data/output/unified_duns_list.xlsx      — same, Excel format
#   data/output/consolidation_report.txt    — run summary
#
# Usage:
#   python scripts/consolidate_duns.py
#   python scripts/consolidate_duns.py --input data/reference --output data/output
#   python scripts/consolidate_duns.py --pattern "DUNS_LTS"
# =============================================================================

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# =============================================================================
# 0. ARGUMENT PARSING
# =============================================================================
parser = argparse.ArgumentParser(description="Consolidate DUNS_LTS JSON files into a unified DUNS list.")
parser.add_argument("--input",   default=None, help="Input directory (default: data/reference relative to this script)")
parser.add_argument("--output",  default=None, help="Output directory (default: data/output relative to this script)")
parser.add_argument("--pattern", default="DUNS_LTS", help="Filename substring to match (default: DUNS_LTS)")
args = parser.parse_args()

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

INPUT_DIR  = Path(args.input)  if args.input  else PROJECT_ROOT / "data" / "reference"
OUTPUT_DIR = Path(args.output) if args.output else PROJECT_ROOT / "data" / "output"
PATTERN    = args.pattern

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 1. DISCOVER JSON FILES
# =============================================================================
json_files = sorted(INPUT_DIR.glob(f"*{PATTERN}*.json"))

if not json_files:
    print(f"[ERROR] No JSON files matching '*{PATTERN}*.json' found in: {INPUT_DIR}")
    sys.exit(1)

print(f"Found {len(json_files)} file(s) matching '*{PATTERN}*.json':")
for f in json_files:
    print(f"  {f.name}")
print()

# =============================================================================
# 2. LOAD AND APPEND ALL FILES
# =============================================================================
frames = []
load_errors: list[tuple[str, str]] = []

for filepath in json_files:
    try:
        # MongoDB JSON exports can be line-delimited (one JSON object per line)
        # or a JSON array. Handle both.
        with open(filepath, "r", encoding="utf-8") as fh:
            content = fh.read().strip()

        if content.startswith("["):
            # Standard JSON array
            records = json.loads(content)
        else:
            # Line-delimited JSON (mongoexport default)
            records = [json.loads(line) for line in content.splitlines() if line.strip()]

        df = pd.DataFrame(records)
        df["_source_file"] = filepath.name
        frames.append(df)
        print(f"  Loaded {len(df):>8,} rows  from  {filepath.name}")

    except Exception as e:
        load_errors.append((filepath.name, str(e)))
        print(f"  [WARN] Failed to load {filepath.name}: {e}")

if not frames:
    print("[ERROR] No files could be loaded.")
    sys.exit(1)

raw = pd.concat(frames, ignore_index=True)
print(f"\nTotal raw rows: {len(raw):,}")

# =============================================================================
# 3. VALIDATE REQUIRED COLUMNS
# =============================================================================
REQUIRED_COLS = {"CS", "duns"}
FIRST_VALUE_COLS = ["BU", "IDS", "SDS"]

missing = REQUIRED_COLS - set(raw.columns)
if missing:
    print(f"\n[ERROR] Required columns missing from data: {missing}")
    print(f"  Columns found: {list(raw.columns)}")
    sys.exit(1)

present_first_value_cols = [c for c in FIRST_VALUE_COLS if c in raw.columns]
missing_optional = set(FIRST_VALUE_COLS) - set(present_first_value_cols)
if missing_optional:
    print(f"[WARN] Optional columns not found (will be skipped): {missing_optional}")

# =============================================================================
# 4. NORMALISE
# =============================================================================
# Strip whitespace from string columns
for col in ["CS", "duns"] + present_first_value_cols:
    if col in raw.columns:
        raw[col] = raw[col].astype(str).str.strip()
        raw[col] = raw[col].replace("nan", pd.NA)

# =============================================================================
# 5. CONSOLIDATE
# =============================================================================
print("\nConsolidating...")

# For each unique CS:
#   - Collect all unique non-null duns values as a sorted list
#   - Keep the first non-null value for BU, IDS, SDS

def first_non_null(series):
    non_null = series.dropna()
    non_null = non_null[non_null.astype(str).str.strip() != ""]
    return non_null.iloc[0] if len(non_null) > 0 else pd.NA

def collect_duns(series):
    vals = series.dropna()
    vals = vals[vals.astype(str).str.strip() != ""]
    unique_vals = sorted(set(vals.astype(str).str.strip()))
    return "|".join(unique_vals) if unique_vals else pd.NA

agg_dict = {
    "duns": collect_duns,
    "_source_file": lambda x: "|".join(sorted(set(x.dropna())))
}
for col in present_first_value_cols:
    agg_dict[col] = first_non_null

unified = (
    raw
    .groupby("CS", sort=True, dropna=False)
    .agg(agg_dict)
    .reset_index()
)

# Add a helper column: count of distinct duns IDs per CS
unified["duns_count"] = unified["duns"].apply(
    lambda x: len(str(x).split("|")) if pd.notna(x) else 0
)

# Reorder columns: CS first, then duns and duns_count, then the rest
col_order = (
    ["CS", "duns", "duns_count"]
    + present_first_value_cols
    + ["_source_file"]
)
unified = unified[[c for c in col_order if c in unified.columns]]

print(f"  Unique CS values   : {len(unified):,}")
print(f"  CS with 1 DUNS     : {(unified['duns_count'] == 1).sum():,}")
print(f"  CS with 2+ DUNS    : {(unified['duns_count'] > 1).sum():,}")
print(f"  CS with no DUNS    : {unified['duns'].isna().sum():,}")

# =============================================================================
# 6. WRITE OUTPUTS
# =============================================================================
csv_path  = OUTPUT_DIR / "unified_duns_list.csv"
xlsx_path = OUTPUT_DIR / "unified_duns_list.xlsx"
report_path = OUTPUT_DIR / "consolidation_report.txt"

unified.to_csv(csv_path, index=False, encoding="utf-8-sig")
print(f"\nWritten: {csv_path}")

try:
    unified.to_excel(xlsx_path, index=False, engine="openpyxl")
    print(f"Written: {xlsx_path}")
except ImportError:
    print("[WARN] openpyxl not installed — skipping Excel output. Run: pip install openpyxl")

# =============================================================================
# 7. CONSOLIDATION REPORT
# =============================================================================
run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

report_lines = [
    "=" * 70,
    " DUNS ID Consolidation Report",
    f" Run timestamp  : {run_ts}",
    f" Pattern matched: *{PATTERN}*.json",
    f" Input dir      : {INPUT_DIR}",
    f" Output dir     : {OUTPUT_DIR}",
    "=" * 70,
    "",
    f"Files processed    : {len(frames)}",
    f"Total raw rows     : {len(raw):,}",
    f"Unique CS values   : {len(unified):,}",
    f"CS with 1 DUNS     : {(unified['duns_count'] == 1).sum():,}",
    f"CS with 2+ DUNS    : {(unified['duns_count'] > 1).sum():,}",
    f"CS with no DUNS    : {unified['duns'].isna().sum():,}",
    "",
    "Files loaded:",
]
for f in json_files:
    report_lines.append(f"  {f.name}")
if load_errors:
    report_lines.append("")
    report_lines.append("Load errors:")
    for fname, err in load_errors:
        report_lines.append(f"  {fname}: {err}")

report_lines += ["", "=" * 70]

with open(report_path, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))
print(f"Written: {report_path}")

print("\nDone.")
