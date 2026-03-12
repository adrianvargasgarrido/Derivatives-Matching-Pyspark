# DUNS ID Engine — Matching Rules

> **Status:** Draft — to be updated as logic is defined.

---

## Rule Hierarchy

Matching is applied in priority order. Once a record is matched by a rule, it is removed from the unmatched pool.

| Priority | Rule Name | Match Keys | Notes |
|---|---|---|---|
| R1 | Exact DUNS match | `duns_id` (internal) = `duns` (reference) | Highest confidence |
| R2 | Tax / Registration ID match | `tax_id` = `registration_number` (reference) | High confidence |
| R3 | Exact legal name match | `entity_name` = `primary_name` (reference) | Case-insensitive, trimmed |
| R4 | Fuzzy name match | Token-sorted ratio ≥ threshold | Lower confidence — flagged for review |

---

## Confidence Scoring

| Rule | Confidence Label | Review Required? |
|---|---|---|
| R1 | `HIGH` | No |
| R2 | `HIGH` | No |
| R3 | `MEDIUM` | Optional |
| R4 | `LOW` | Yes — written to `review/` |

---

## Unmatched

Records that do not satisfy any rule are written to `data/output/unmatched.csv` with a reason code:

| Reason Code | Description |
|---|---|
| `NO_KEY_FOUND` | No matching key exists in reference data |
| `BELOW_THRESHOLD` | Fuzzy score below minimum threshold |
| `AMBIGUOUS` | Multiple candidates with equal score |

---

## Parameters (to be configured in notebook Section 1)

```python
FUZZY_THRESHOLD = 85          # Minimum token-sort ratio for R4 (0–100)
MAX_CANDIDATES_PER_RECORD = 3 # Max reference candidates to evaluate per input record
REVIEW_CONFIDENCE = "LOW"     # Confidence level that triggers review/ output
```
