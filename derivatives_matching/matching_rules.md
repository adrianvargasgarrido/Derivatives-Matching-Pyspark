1) Your matching rules (from the screenshots)

All rules use:
	•	Axis amount: SACCRMTM
	•	Finstore amount: gbpequivalentamount
	•	SOPHIS-only rules have filter_sophis=True

SOPHIS-specific (priority 1–3)

Priority
BRD rule
Axis keys
Finstore keys
Notes
1
SOPHIS #1
DerivedSophisId
fissnumber
SOPHIS only
2
SOPHIS #2
CompositeKey_SophisId_BookId
CompositeKey_Fissnumber_TradingSystemBook
SOPHIS only
3
SOPHIS #3
DerivedSophisId
tradeid
SOPHIS only


OTC rules (priority 4–13)
Priority
BRD rule
Axis keys
Finstore keys
Notes
4
OTC #1 “QUICK WIN”
SourceSystemTradeId
tradeid
marked NEW
5
OTC #2
CompositeKey_TradeId_MasterBookId
CompositeKey_TradeId_MasterBookId
NEW
6
OTC #3
SourceSystemTradeId
alternatesummittradeid
7
OTC #4
CompositeKey_TradeId_MasterBookId
CompositeKey_AlternateSummitTradeId_MasterBookId
8
OTC #5
DerivedSophisId
fissnumber
SOPHIS-style join but applied to all
9
OTC #6
CompositeKey_SophisId_BookId
CompositeKey_Fissnumber_TradingSystemBook
10
OTC #7
DerivedSophisId
tradeid
11
OTC #8
DerivedDelta1Id
tradeid
12
OTC #9
SourceSystemTradeId
alternatetradeid1
13
OTC #10
CompositeKey_TradeId_MasterBookId
CompositeKey_AlternateTradeId1_MasterBookId

2) What breaks at scale if you “port the notebook to PySpark” directly

If you implement this literally as:
	•	join for rule i
	•	collect matched ids
	•	anti-join to remove matched from pools
	•	repeat 13 times

You’ll hit these problems:

A) 13 big joins + 13 big anti-joins = repeated shuffles

Each rule can trigger a wide shuffle. Anti-joins on tens of millions are also heavy. Doing this 13 times becomes hours/days.

B) Many-to-many explosions on “non-unique keys”

Keys like DerivedSophisId or fissnumber may not be unique.
A simple equality join can generate huge candidate sets (Cartesian within a key), which will blow up.

C) Carrying 100+ columns through joins makes shuffles enormous

If you join with full schemas, every shuffle moves very wide rows. This is a common hidden killer.

D) Skew (hot keys) creates stragglers / OOM

Some trade IDs / counterparties / books may dominate. One partition becomes massive → one task runs forever or dies.

⸻

3) Best-practice Spark architecture for your waterfall rules

The winning pattern: Candidate edges → global ranking → 1–1 resolution

Instead of mutating pools 13 times, do:
	1.	For each rule, generate a candidate edges table:
	•	(axis_id, fin_id, priority, rule_name, optional metrics)
	2.	Union all candidates
	3.	Enforce waterfall behavior with window ranking (priority first)
	4.	Join back wide columns at the end

This gives you the same effect as “remove matched after each rule” but with far fewer expensive steps.

⸻

4) The key implementation changes you should make

Change 1 — Split data into CORE vs WIDE

Do matching on core columns only:

axis_core:
	•	axis_id, SourceSystemName, SourceSystemTradeId, BookId, DerivedSophisId, DerivedDelta1Id, CounterpartyId, SACCRMTM, plus precomputed composite keys

fin_core:
	•	fin_id, tradeid, alternatetradeid1, alternatesummittradeid, masterbookid, tradingsystembook, fissnumber, counterpartyid, gbpequivalentamount, plus composite keys

Keep your 100+ extra columns in axis_wide / fin_wide keyed by axis_id / fin_id.

Change 2 — Precompute composite keys once (no UDF)

Use Spark SQL functions:
	•	CompositeKey_SophisId_BookId = concat_ws('_', DerivedSophisId, BookId)
	•	CompositeKey_Fissnumber_TradingSystemBook = concat_ws('_', fissnumber, tradingsystembook)
	•	CompositeKey_TradeId_MasterBookId = concat_ws('_', SourceSystemTradeId, BookId)
and in finstore concat_ws('_', tradeid, masterbookid) (same name but different source cols)
	•	etc.

Change 3 — Generate candidates per rule using projections

For each rule:
	•	select only axis_id + required join keys
	•	select only fin_id + required join keys
	•	join
	•	emit (axis_id, fin_id, priority, rule_name)

Change 4 — Resolve 1–1 with windows (this replaces the “remove matched pools” loop)

You want:
	•	best fin per axis by priority
	•	then best axis per fin by priority

That replicates waterfall + prevents reuse.

⸻

5) PySpark skeleton that matches your rule dataclass

Below is a practical structure you can paste into VS Code and build on.

Rule structure (same idea as your dataclass)

MATCHING_RULES = [
  # SOPHIS
  dict(priority=1, rule="SOPHIS #1", axis_keys=["DerivedSophisId"], fin_keys=["fissnumber"], filter_sophis=True),
  dict(priority=2, rule="SOPHIS #2", axis_keys=["CompositeKey_SophisId_BookId"], fin_keys=["CompositeKey_Fissnumber_TradingSystemBook"], filter_sophis=True),
  dict(priority=3, rule="SOPHIS #3", axis_keys=["DerivedSophisId"], fin_keys=["tradeid"], filter_sophis=True),

  # OTC
  dict(priority=4, rule="OTC #1 QUICK WIN", axis_keys=["SourceSystemTradeId"], fin_keys=["tradeid"], filter_sophis=False),
  dict(priority=5, rule="OTC #2", axis_keys=["CompositeKey_TradeId_MasterBookId"], fin_keys=["CompositeKey_TradeId_MasterBookId"], filter_sophis=False),
  dict(priority=6, rule="OTC #3", axis_keys=["SourceSystemTradeId"], fin_keys=["alternatesummittradeid"], filter_sophis=False),
  dict(priority=7, rule="OTC #4", axis_keys=["CompositeKey_TradeId_MasterBookId"], fin_keys=["CompositeKey_AlternateSummitTradeId_MasterBookId"], filter_sophis=False),
  dict(priority=8, rule="OTC #5", axis_keys=["DerivedSophisId"], fin_keys=["fissnumber"], filter_sophis=False),
  dict(priority=9, rule="OTC #6", axis_keys=["CompositeKey_SophisId_BookId"], fin_keys=["CompositeKey_Fissnumber_TradingSystemBook"], filter_sophis=False),
  dict(priority=10, rule="OTC #7", axis_keys=["DerivedSophisId"], fin_keys=["tradeid"], filter_sophis=False),
  dict(priority=11, rule="OTC #8", axis_keys=["DerivedDelta1Id"], fin_keys=["tradeid"], filter_sophis=False),
  dict(priority=12, rule="OTC #9", axis_keys=["SourceSystemTradeId"], fin_keys=["alternatetradeid1"], filter_sophis=False),
  dict(priority=13, rule="OTC #10", axis_keys=["CompositeKey_TradeId_MasterBookId"], fin_keys=["CompositeKey_AlternateTradeId1_MasterBookId"], filter_sophis=False),
]
from pyspark.sql import functions as F

def build_candidates_for_rule(axis_core, fin_core, rule):
    a = axis_core
    if rule["filter_sophis"]:
        # adapt this filter to your real SOPHIS definition
        a = a.filter(F.col("SourceSystemName").like("SOPHIS%"))

    # Project only needed columns (minimize shuffle payload)
    a_sel = a.select(["axis_id"] + rule["axis_keys"] + ["SACCRMTM"])
    f_sel = fin_core.select(["fin_id"] + rule["fin_keys"] + ["gbpequivalentamount"])

    # Join condition: axis_keys[i] == fin_keys[i]
    cond = None
    for ak, fk in zip(rule["axis_keys"], rule["fin_keys"]):
        c = (F.col(f"ak_{ak}") == F.col(f"fk_{fk}")) if False else None

    # Easiest: rename to common names then join
    a_ren = a_sel
    f_ren = f_sel
    for i, (ak, fk) in enumerate(zip(rule["axis_keys"], rule["fin_keys"])):
        a_ren = a_ren.withColumnRenamed(ak, f"k{i}")
        f_ren = f_ren.withColumnRenamed(fk, f"k{i}")

    join_cols = [f"k{i}" for i in range(len(rule["axis_keys"]))]

    joined = a_ren.join(f_ren, on=join_cols, how="inner")

    # Optional tie-break metric: amount difference
    joined = joined.withColumn("amount_diff", F.abs(F.col("SACCRMTM") - F.col("gbpequivalentamount")))

    return joined.select(
        "axis_id", "fin_id", "amount_diff",
        F.lit(rule["priority"]).alias("priority"),
        F.lit(rule["rule"]).alias("rule_name"),
        F.lit("deterministic").alias("rule_type")
    )

candidates = None
for r in MATCHING_RULES:
    c = build_candidates_for_rule(axis_core, fin_core, r)
    candidates = c if candidates is None else candidates.unionByName(c)

from pyspark.sql.window import Window

# 1) Best fin per axis
w_axis = Window.partitionBy("axis_id").orderBy(
    F.col("priority").asc(),
    F.col("amount_diff").asc_nulls_last(),
    F.col("fin_id").asc()
)
best_per_axis = candidates.withColumn("rn_axis", F.row_number().over(w_axis)).filter("rn_axis = 1")

# 2) Best axis per fin (prevents fin reuse)
w_fin = Window.partitionBy("fin_id").orderBy(
    F.col("priority").asc(),
    F.col("amount_diff").asc_nulls_last(),
    F.col("axis_id").asc()
)
matches = best_per_axis.withColumn("rn_fin", F.row_number().over(w_fin)).filter("rn_fin = 1") \
    .select("axis_id", "fin_id", "priority", "rule_name", "amount_diff")

final = matches \
    .join(axis_wide, on="axis_id", how="left") \
    .join(fin_wide, on="fin_id", how="left")


6) The big “gotcha” you MUST handle: many-to-many candidate explosions

Rules like:
	•	DerivedSophisId = fissnumber
	•	DerivedSophisId = tradeid

can create huge candidate sets if those fields are not unique.

Best practice protections:

Protection A — Pre-check join key cardinality

Before running a rule on full data, compute:
	•	count per key on each side
	•	if axis_count * fin_count is too large for many keys, you need additional constraints

Protection B — Add safe secondary constraints where possible

Examples:
	•	include BookId/masterbookid (you already do this in rules 2, 9)
	•	include system filters (SOPHIS-only)
	•	include currency/date window if available

Protection C — Hard cap candidates per key (safety valve)

If a key produces 10k matches, you probably don’t want all 10k.
You can cap by:
	•	smallest amount_diff
	•	or pick top N per (key) before global resolution

⸻

7) How this maps to your current logic exactly

Your current waterfall logic:
	•	execute rule 1, remove matched
	•	execute rule 2 on remaining
	•	etc.

The priority-first ranking replicates that:
	•	lower priority always beats higher priority
	•	one-to-one is enforced by the second window pass
	•	you avoid 13 pool mutations and repeated anti-joins
