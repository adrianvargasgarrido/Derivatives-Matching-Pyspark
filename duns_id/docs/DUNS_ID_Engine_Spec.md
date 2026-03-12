# DUNS ID Engine — Technical Specification

**Version:** v1.0 (Draft)  
**Date:** 2026-03-12  

---

## 1. Purpose

Enrich and validate **DUNS identifiers** on internal entity records by matching them against a reference dataset sourced from D&B or equivalent provider.

---

## 2. Inputs

### 2.1 Reference Data (JSON)
- Downloaded from the D&B API or batch export
- Stored in `data/reference/`
- Key fields (to be confirmed):
  - `duns` — 9-digit DUNS number
  - `primary_name` — legal entity name
  - `registration_number` — tax / company registration ID
  - `country` — ISO 2-letter country code
  - `status` — active / inactive

### 2.2 Input Records (Excel / CSV)
- Uploaded from internal system
- Stored in `data/input/`
- Key fields (to be confirmed):
  - `entity_id` — internal entity identifier
  - `entity_name` — entity name as held internally
  - `duns_id` — existing DUNS ID if already known (may be null or stale)
  - `tax_id` — tax / registration number
  - `country` — country of incorporation

---

## 3. Matching Logic

See `matching_rules.md` for the full rule hierarchy. At a high level:

1. **Exact key matching** — DUNS ID, Tax ID
2. **Exact name matching** — case-insensitive normalised legal name
3. **Fuzzy name matching** — token-sorted string similarity above a configurable threshold
4. **Unmatched** — written out separately with reason codes

---

## 4. Outputs

| Output File | Description |
|---|---|
| `data/output/enriched_records.csv` | All input records with DUNS ID appended / confirmed |
| `data/output/unmatched.csv` | Records that could not be matched |
| `data/output/run_summary.json` | Aggregate statistics for the run |
| `review/review_package.xlsx` | Low-confidence matches requiring human sign-off |

---

## 5. Configuration

All configuration lives in **Section 1** of the notebook:

```python
# --- Paths ---
INPUT_DIR       = "data/input/"
REFERENCE_DIR   = "data/reference/"
OUTPUT_DIR      = "data/output/"
REVIEW_DIR      = "review/"

# --- Run metadata ---
RUN_ID          = "DUNS_RUN_001"
RUN_DATE        = "2026-03-12"

# --- Matching parameters ---
FUZZY_THRESHOLD         = 85
MAX_CANDIDATES          = 3
REVIEW_CONFIDENCE_LEVEL = "LOW"
```

---

## 6. Data Quality Checks

Performed in **Section 4** of the notebook before matching:

| Check | Field | Action on Failure |
|---|---|---|
| Null check | `entity_id` | Reject row, log to DQ report |
| Null check | `entity_name` | Flag, proceed with reduced confidence |
| Format check | `duns_id` | Must be 9 digits if present; otherwise null |
| Duplicate check | `entity_id` | Deduplicate, keep first occurrence |
| Country format | `country` | Must be ISO 2-letter; warn if invalid |

---

## 7. Review Process

Records written to `review/review_package.xlsx` contain:

| Column | Description |
|---|---|
| `entity_id` | Internal ID |
| `entity_name` | Internal name |
| `matched_duns` | Proposed DUNS ID |
| `matched_name` | Reference name at proposed DUNS |
| `confidence_score` | Fuzzy similarity score |
| `confidence_label` | LOW / MEDIUM / HIGH |
| `reviewer_decision` | To be filled: ACCEPT / REJECT / AMEND |
| `reviewer_notes` | Free text |

---

## 8. Auditability

Every output row carries:
- `run_id` — unique identifier for the pipeline run
- `run_date` — date of execution
- `match_rule` — which rule (R1–R4) produced the match
- `confidence_label` — HIGH / MEDIUM / LOW
- `match_timestamp` — UTC timestamp
