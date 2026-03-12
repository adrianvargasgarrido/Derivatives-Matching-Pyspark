Why the current joining/matching logic breaks at scale

1) Repeated joins + “remove matched from pools” = repeated full shuffles

Your waterfall does:
	•	join by keys (rule i)
	•	collect matched keys
	•	anti-join to remove matches
	•	repeat

On big data, each step can trigger:
	•	wide shuffles (especially if keys are not partition-aligned)
	•	expensive anti-joins
	•	repeated scans of the full remaining pool

This is the main performance killer.

2) Any Python loops / groupby-iterrows style logic is a hard no

Greedy Strategy 1 in Pandas is essentially:
	•	group by counterparty
	•	for each row find candidates
	•	choose best

If you implement this with .collect() or mapPartitions without careful blocking, you’ll explode runtime or memory.

3) “100+ columns” makes everything worse

Spark shuffle cost scales with:
	•	number of rows moved
	•	size of each row moved

If you join using full DataFrames with 100+ columns, every shuffle carries huge payloads.
You must project only necessary columns for matching, and join back later.

4) Skew will ruin you (Counterparty, Book, Instrument can be skewed)

A few counterparties or books might dominate volume, causing:
	•	single partitions to become massive
	•	long tail tasks
	•	executor OOM

5) “isin(set)” on 20M is not free

Pandas .isin(set) is cheap enough.
Spark equivalent (col.isin(list)) becomes a big broadcast literal or a join anyway.
For large lists, it’s better to do a semi-join.

⸻

The Spark-native design you want

Core idea: Normalize the matching into key-based candidate generation + ranking

Instead of “join and then remove pools repeatedly”, do:
	1.	For each rule, create a candidate edge table:
	•	axis_id, fin_id, rule_priority, maybe amount_diff
	2.	Union all candidate edges across rules
	3.	Pick the best match per Axis and per Finstore using window ranking (enforces 1–1)
	4.	Output matches + unmatched

This removes the repeated “pool mutation” loop and replaces it with:
	•	a small number of large joins (one per rule or grouped rules)
	•	one global ranking phase
	•	one final join-back to enrich with 100 columns

You still preserve the waterfall priority (priority is just a sort key).

⸻

Concrete changes to implement (high impact)

Change 1: Add stable IDs + narrow the working schema

Before any matching:
	•	Add axis_row_id (unique) and fin_row_id
	•	Keep only needed columns in the matching phase:
	•	keys used in rules
	•	amount (SACCMTM / gbpequivalentamount)
	•	counterparty
	•	system
	•	book
	•	instrument
	•	Park the “wide” 100+ columns in separate DataFrames for later enrichment

Pattern
	•	axis_core = ids + matching columns (maybe 10–25 columns)
	•	axis_wide = ids + remaining columns
Same for finstore.

This alone reduces shuffle size dramatically.

⸻

Change 2: Precompute derived keys once (no repeated UDF work)

Compute:
	•	DerivedSophisId, DerivedDelta1Id
	•	any composite keys needed (e.g., tradeid_book, instrument_book)
	•	amount bucket for Strategy 2

Use Spark SQL functions (avoid Python UDF):
	•	split(col, '-'), element_at, concat_ws, floor, abs, etc.

⸻

Change 3: Implement Layer 1 as candidate edges + rank, not iterative removal

For each rule i:
	•	Build two “key tables”:
	•	axis_rule_i = axis_core.select(axis_row_id, key_cols...)
	•	fin_rule_i = fin_core.select(fin_row_id, key_cols...)
	•	Join on those key cols
	•	Output: axis_row_id, fin_row_id, rule_priority=i, match_category, maybe extra

Then:
	•	candidates_layer1 = unionAll(rule_candidates_i)

Finally enforce 1–1 and priority:

Ranking logic
	1.	Keep best fin for each axis (min priority)
	2.	Then keep best axis for each fin (min priority)
	3.	If collisions remain, repeat with stable tie-breaker (e.g., smallest amount_diff, then smallest ids)

In practice you can do a two-pass window approach that works well:
	•	Window by axis_row_id order by priority, tie_breaker
	•	Filter row_number()==1
	•	Then window by fin_row_id on that reduced set
	•	Filter row_number()==1

This mimics “remove matched from pools”.

⸻

Change 4: Strategy 1 greedy = join on counterparty + filter tolerance + rank by diff

Replace loop with:
	1.	Join remaining axis vs remaining fin on counterpartyid
	2.	Compute:
	•	diff = abs(axis_amount - fin_amount)
	•	tol = abs(axis_amount) * 0.01
	3.	Filter diff <= tol
	4.	Pick best per axis (min diff), then ensure best per fin

Important: you must run this only on the unmatched subset after Layer 1 (or integrate it as later priority candidates).

⸻

Change 5: Strategy 2 greedy = blocking join using amount buckets

Do NOT compare all-to-all by amount.
	1.	bucket = floor(amount / 1000) * 1000 (or for negatives handle consistently)
	2.	Join on bucket and nearby buckets
	•	Use a small “bucket expansion” on the axis side:
	•	axis produces 3 rows per trade: bucket-1000, bucket, bucket+1000
	3.	Join axis_expanded to fin on expanded_bucket
	4.	Filter strict tolerance <= 0.1%
	5.	Rank like strategy 1

This makes strategy 2 feasible.

⸻

Change 6: Use broadcast joins only when truly small

Rules where one side is small:
	•	ETD system subset might be small
	•	Some scope-limited systems

But do not broadcast big tables (20M rows) accidentally.
Explicitly control:
	•	broadcast(fin_subset) only if it’s genuinely small
	•	otherwise rely on partitioned shuffle join

⸻

Partitioning / clustering strategy (critical)

Partition by the main join keys

For Layer 1 rules, your biggest joins are:
	•	SourceSystemTradeId ↔ tradeid
	•	DerivedSophisId ↔ fissnumber
	•	instrumentid joins
	•	composite keys with book

In Databricks/Delta, you want:
	•	Delta tables with Z-ORDER on the join keys
	•	Use repartition(key) before big joins where it helps
	•	Cache only small, heavily reused intermediate tables

For Greedy:
	•	Strategy 1: repartition by counterpartyid
	•	Strategy 2: repartition by amount_bucket

Skew handling

If counterpartyid is skewed:
	•	Use Spark AQE + skew join handling if enabled
	•	Or salt keys for heavy counterparties:
	•	salt = hash(axis_row_id) % N
	•	join on (counterpartyid, salt) with replicated salts on fin

⸻

Handling 100+ columns efficiently

Rule: “match narrow, enrich wide”
	1.	Perform all joins and ranking using only core columns
	2.	Produce a final match table with:
	•	axis_row_id, fin_row_id, priority, match_type, diff, etc.
	3.	Join back to wide tables at the end:
	•	final = matches.join(axis_wide, 'axis_row_id').join(fin_wide, 'fin_row_id')

This avoids shuffling 100 columns through every intermediate stage.

Also consider storing wide as Delta and only selecting needed output columns for exports.

⸻

File format changes (massive impact)

If you’re still on CSV:
	•	CSV is slow, non-splittable (or poorly splittable), no schema, heavy parsing
	•	Writing huge CSV is painful and often bottlenecks everything

Use:
	•	Delta/Parquet for all intermediate outputs
	•	Convert to CSV only at the very end and only if needed (and maybe per-partition)

⸻

Practical “rewrite plan” (what you actually implement)

Step A — Build core tables
	•	axis_core, fin_core (few columns)
	•	axis_wide, fin_wide (everything else)

Step B — Layer 1 candidates
	•	For each rule: build candidates_rule_i
	•	Union all candidates
	•	Resolve 1–1 with window ranking (priority first)

Step C — Compute unmatched
	•	anti-join axis_core vs matched axis ids
	•	anti-join fin_core vs matched fin ids

Step D — Greedy candidates
	•	Strategy 1 candidates from unmatched subset
	•	Strategy 2 candidates from unmatched subset
	•	Union them with priorities after deterministic (e.g., 100+)

Step E — Final resolution
	•	union layer1_matches + greedy_matches candidates
	•	run final 1–1 resolution if needed (or do greedy only on remaining, easier)

Step F — Enrich and write outputs
	•	join to wide tables
	•	write Delta outputs
	•	optionally export CSV samples

⸻

The “gotchas” to avoid
	•	Python UDFs for key derivation or bucketing → use built-in functions
	•	collect() of keys to driver to filter → do joins instead
	•	caching huge DataFrames blindly → you’ll evict and thrash
	•	joining wide tables early → you’ll pay shuffle tax repeatedly
	•	assuming rule-by-rule iterative pool updates is required → it’s not; priority ranking replicates it

⸻

If you want, I can give you a Spark skeleton

If you send me:
	•	the exact rule definitions structure (how rules are represented)
	•	the key columns per rule (from your BRD table)
	•	your current “execute_matching_rule” code snippet

…I’ll produce:
	•	a PySpark implementation skeleton for:
	•	generate_rule_candidates(rule)
	•	resolve_one_to_one(candidates)
	•	run_layer1(), run_greedy()
	•	plus recommended repartition points and columns to Z-ORDER.


