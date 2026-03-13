# DUNS ID Matching Engine

**Version:** v1.1  
**Date:** 2026-03-13  
**Status:** 🚧 In Development  

---

## Overview

This project enriches and validates **DUNS (Data Universal Numbering System) identifiers** across internal systems. It ingests structured data from multiple upstream sources (JSON downloads, Excel uploads), applies a matching and enrichment pipeline, and produces reviewed, auditable outputs.

---

## Repository Structure

```
duns_id/
│
├── notebooks/                       # Jupyter / Databricks notebooks
│   └── DUNS_ID_Enrichment.ipynb     # Main DUNS enrichment notebook
│
├── scripts/
│   ├── DunsConfig.ps1               # Shared config for PowerShell scripts
│   ├── Export-DunsCollections.ps1    # PowerShell: bulk export from MongoDB
│   ├── Preview-DunsCollections.ps1   # PowerShell: stats + interactive export
│   ├── Run-Duns.ps1                  # PowerShell: simple launcher
│   └── consolidate_duns.py           # Python: merge DUNS_LTS JSON → unified list
│
├── src/
│   └── fca/                          # FCA Broker Lookup module (Step 3)
│       ├── __init__.py
│       ├── config.py                 # Load API config from .env
│       ├── client.py                 # HTTP client for FCA gateway
│       ├── parser.py                 # Normalise raw API responses
│       └── run_fca_lookup.py         # CLI entry point for FCA lookups
│
├── data/
│   ├── input/                        # Raw uploads (Excel, CSV)
│   │   └── excel_files/              # Excel files for enrichment
│   ├── reference/                    # Downloaded JSON from MongoDB
│   └── output/                       # Processed results
│       └── fca/                      # FCA lookup results
│
├── review/                           # Outputs pending manual review
│
├── tests/
│   └── test_duns_utils.py            # Unit tests
│
├── docs/
│   └── DUNS_ID_Engine_Spec.md        # Technical specification
│
├── .env.example                      # FCA API config template (copy → .env)
├── requirements.txt                  # Python dependencies
├── project_notes.md                  # Running notes and decisions log
├── matching_rules.md                 # Documented matching logic
└── CHANGELOG.md                      # Version history
```

---

## Data Flows

### Step 0 — Download reference data from MongoDB

Run the export script on **Windows** (PowerShell) to pull JSON collections from the DEV or TEST DUNS database:

```powershell
# Export default collection list from DEV (prompts for password)
.\scripts\Export-DunsCollections.ps1

# Export from TEST environment
.\scripts\Export-DunsCollections.ps1 -Environment TEST

# Export specific collections only
.\scripts\Export-DunsCollections.ps1 -Collections "BARC_CLNT_OWNRSHP_HRCHY_duns","BARC_ENTITY_MASTER"
```

Output files are automatically named `<CollectionName>_DEV.json` or `<CollectionName>_TEST.json` and saved to `data/reference/`.  
This means DEV and TEST exports **never overwrite each other**.

See [`scripts/Export-DunsCollections.ps1`](scripts/Export-DunsCollections.ps1) for full parameter reference.

---

### Step 1 — Consolidate DUNS reference data

Merge all `*DUNS_LTS*.json` files from `data/reference/` into a single unified list:

```bash
python scripts/consolidate_duns.py
```

Outputs: `data/output/unified_duns_list.csv`, `.xlsx`, and `consolidation_report.txt`.

---

### Step 2 — Enrich input with DUNS IDs

Open `notebooks/DUNS_ID_Enrichment.ipynb`, configure the Excel filename + join key, and run all cells.  
Produces enriched CSV/Excel, unmatched review file, and match summary report.

---

### Step 3 — FCA Broker Lookup

Use the FCA module to search the FCA register by company name (`D_CUSTOMER_NAME`) or fetch addresses directly by FCA Reference Number.

**Setup:**

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create your .env from the template
cp .env.example .env
# Edit .env and fill in your actual bearer token + auth key
```

**Usage (run from the duns_id/ folder):**

```bash
# Search by D_CUSTOMER_NAME (default)
python -m src.fca.run_fca_lookup --input data/input/my_companies.csv --column D_CUSTOMER_NAME

# Search by any other name column
python -m src.fca.run_fca_lookup --input data/output/enriched_file.csv --column company_name

# Lookup by FCA Reference Number directly (skip search)
python -m src.fca.run_fca_lookup --mode frn --input data/output/enriched_file.csv --column reference_number

# Custom output folder
python -m src.fca.run_fca_lookup --output data/output/fca_round2
```

**Outputs** (saved to `data/output/fca/`):

| File | Description |
|---|---|
| `search_results.csv` | All FCA firm search hits (name, FRN, status, type) |
| `address_results.csv` | Registered addresses for each FRN |
| `merged_results.csv` | Search + address joined into one flat file |
| `errors.csv` | Failed lookups with stage + error detail |

---

### Inputs

| Source | Format | Folder | Description |
|---|---|---|---|
| MongoDB (DEV) | JSON | `data/reference/` | `*_DEV.json` — downloaded via export script |
| MongoDB (TEST) | JSON | `data/reference/` | `*_TEST.json` — downloaded via export script |
| Internal system upload | Excel (`.xlsx`) | `data/input/` | Entity records to be enriched |
| Internal system upload | CSV | `data/input/` | Alternative flat file inputs |

### Outputs

| Output | Format | Folder | Description |
|---|---|---|---|
| Enriched entity records | CSV / Excel | `data/output/` | Matched records with DUNS IDs appended |
| Unmatched records | CSV | `data/output/` | Records that could not be matched |
| Review package | Excel | `review/` | Curated file for manual review / sign-off |
| Match summary report | TXT | `data/output/` | Aggregate statistics per run |
| FCA search results | CSV | `data/output/fca/` | FCA firm search hits |
| FCA address results | CSV | `data/output/fca/` | FCA registered addresses |
| FCA merged results | CSV | `data/output/fca/` | Search + address joined |
| FCA errors | CSV | `data/output/fca/` | Failed lookups with error detail |

---

## Pipeline Steps

| Step | Script / Notebook | Description |
|---|---|---|
| **0** | `scripts/Export-DunsCollections.ps1` | Download reference JSON from MongoDB |
| **1** | `scripts/consolidate_duns.py` | Merge `*DUNS_LTS*.json` → `unified_duns_list.csv` |
| **2** | `notebooks/DUNS_ID_Enrichment.ipynb` | Enrich Excel input with DUNS IDs, produce match summary |
| **3** | `src/fca/run_fca_lookup.py` | FCA Broker Lookup — search by `D_CUSTOMER_NAME` or `reference_number` |

---

### Step 0 — Download reference data from MongoDB

## Notebook — `DUNS_ID_Enrichment.ipynb`

The enrichment notebook is divided into self-contained sections:

| Section | Description |
|---|---|
| 1 | Imports & Paths — pandas, pathlib, run timestamp |
| 2 | Load Input Excel File |
| 3 | Load Unified DUNS Reference |
| 4 | Configure Join Keys (`EXCEL_CLIENT_COL` ↔ `CS`) |
| 5 | Match & Enrich — left join, flag MATCHED / UNMATCHED |
| 6 | Match Summary — row-level + unique `CUSTOMER_IDENTIFIER` breakdown |
| 7 | Export — enriched CSV/Excel, unmatched review file, text report |

---

## Getting Started

1. **Install dependencies** — `pip install -r requirements.txt`
2. **Download reference data** — run `scripts/Export-DunsCollections.ps1` (Windows / PowerShell)
3. **Consolidate DUNS data** — `python scripts/consolidate_duns.py`
4. **Place input files** — copy Excel / CSV uploads into `data/input/`
5. **Run enrichment notebook** — `notebooks/DUNS_ID_Enrichment.ipynb`
6. **Run FCA lookup** — `python -m src.fca.run_fca_lookup` (from `duns_id/`)
7. **Collect outputs** from `data/output/` and review files from `review/`
