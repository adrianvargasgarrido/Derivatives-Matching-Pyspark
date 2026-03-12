# DUNS ID Matching Engine

**Version:** v1.0  
**Date:** 2026-03-12  
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
│   └── DUNS_ID_Matching.ipynb       # Main processing notebook
│
├── scripts/
│   └── Export-DunsCollections.ps1   # PowerShell: bulk export collections from MongoDB (DEV or TEST)
│
├── data/
│   ├── input/                       # Raw uploads from source systems (Excel, CSV)
│   ├── reference/                   # Downloaded JSON files from MongoDB (output of export script)
│   └── output/                      # Processed results ready for review
│
├── review/                          # Outputs pending manual review / sign-off
│
├── src/
│   └── duns_utils.py                # Reusable Python functions (parsing, cleaning, matching)
│
├── tests/
│   └── test_duns_utils.py           # Unit tests for src/ functions
│
├── docs/
│   └── DUNS_ID_Engine_Spec.md       # Technical specification
│
├── project_notes.md                 # Running notes and decisions log
├── matching_rules.md                # Documented matching logic
└── CHANGELOG.md                     # Version history
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
| Match summary report | CSV / JSON | `data/output/` | Aggregate statistics per run |

---

## Notebook — `DUNS_ID_Matching.ipynb`

The main notebook is divided into self-contained sections:

| Section | Description |
|---|---|
| 1 | Configuration — paths, parameters, run metadata |
| 2 | Ingest reference data — load and parse JSON downloads |
| 3 | Ingest input data — load Excel / CSV uploads |
| 4 | Data quality checks — nulls, formats, duplicates |
| 5 | Pre-processing — normalise names, clean IDs |
| 6 | Matching logic — apply matching rules |
| 7 | Post-matching review — flag low-confidence matches |
| 8 | Output — write enriched records, unmatched, review package |
| 9 | Summary report |

---

## Getting Started

1. **Download reference data** — run `scripts/Export-DunsCollections.ps1` (Windows / PowerShell) to pull JSON files into `data/reference/`
2. **Place input files** — copy Excel / CSV uploads into `data/input/`
3. **Open the notebook** — `notebooks/DUNS_ID_Matching.ipynb`
4. **Update configuration** in **Section 1** (paths, environment, run metadata)
5. **Run all cells** top to bottom
6. **Collect outputs** from `data/output/` and review files from `review/`
