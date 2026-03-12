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
  ├── JSON reference files     ← downloaded from D&B / API
  └── Excel / CSV uploads      ← uploaded from internal system

↓ Ingest & Parse

↓ Data Quality Checks

↓ Pre-processing (name normalisation, ID cleaning)

↓ Matching Layer
      ├── Exact match on entity ID / tax ID
      ├── Fuzzy name match (fallback)
      └── Manual review flag (low confidence)

↓ Output
      ├── data/output/  ← enriched records, unmatched list, summary
      └── review/       ← items needing human sign-off
```

---

## 4. Decisions Log

| Date | Decision | Reason |
|---|---|---|
| 2026-03-12 | Project created as sub-folder within monorepo | Related domain, shared best practices |
| | | |

---

## 5. Open Questions / TODO

- [ ] Confirm source of JSON reference data (D&B direct API or batch download?)
- [ ] Confirm columns available in the Excel upload files
- [ ] Define confidence threshold for fuzzy matching
- [ ] Define review/sign-off process for low-confidence matches
- [ ] Decide: Pandas (local) or PySpark (scale)?
