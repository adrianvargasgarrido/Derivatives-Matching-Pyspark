# DUNS ID Project — Notes

---

## 1. Purpose

Enrich internal entity records with validated **DUNS (Data Universal Numbering System) identifiers** sourced from D&B (Dun & Bradstreet) or equivalent reference data.

---

## 2. Background

- DUNS IDs are 9-digit identifiers used globally to uniquely identify business entities.
- Internal systems may hold entity names / IDs that do not yet have a corresponding DUNS ID, or hold stale / incorrect DUNS IDs.
- This pipeline ingests reference data (JSON format) alongside internal records (Excel / CSV), applies matching logic, and produces an enriched output for review.

---

## 3. Architecture (Draft)

```
INPUT
  ├── JSON reference files     ← downloaded from D&B / MongoDB
  ├── Excel / CSV uploads      ← uploaded from internal system
  └── FCA API responses        ← queried from FCA Broker Gateway

↓ Step 0 — Export MongoDB collections (PowerShell)

↓ Step 1 — Consolidate DUNS_LTS JSONs → unified_duns_list.csv

↓ Step 2 — Enrichment Notebook
      ├── Left join Excel ↔ unified DUNS ref
      ├── Match summary (rows + unique CUSTOMER_IDENTIFIER)
      └── Export enriched + unmatched review files

↓ Step 3 — FCA Broker Lookup (src/fca/)
      ├── Search by D_CUSTOMER_NAME  → firm hits
      │   └── For each FRN → fetch registered address
      ├── OR: direct lookup by reference_number
      └── Output: search_results, address_results, merged, errors
```

---

## 4. Decisions Log

| Date | Decision | Reason |
|---|---|---|
| 2026-03-12 | Project created as sub-folder within monorepo | Related domain, shared best practices |
| 2026-03-12 | PowerShell scripts for MongoDB export | Work machine is Windows; need to download JSON reference data |
| 2026-03-12 | consolidate_duns.py for JSON merging | Multiple DUNS_LTS collections need to become one unified list |
| 2026-03-12 | Enrichment notebook (pandas) | Quick local exploration; no Spark needed for this volume |
| 2026-03-13 | FCA Broker Lookup module (`src/fca/`) | Need FCA reference numbers / addresses for customers — lookup by `D_CUSTOMER_NAME` or existing `reference_number` |
| | | |

---

## 5. Open Questions / TODO

- [ ] Confirm source of JSON reference data (D&B direct API or batch download?)
- [ ] Confirm columns available in the Excel upload files
- [ ] Define confidence threshold for fuzzy matching
- [ ] Define review/sign-off process for low-confidence matches
- [ ] Decide: Pandas (local) or PySpark (scale)?
