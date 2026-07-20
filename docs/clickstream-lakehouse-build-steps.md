# Clickstream Lakehouse — Granular Build Sequence

Companion to `clickstream-lakehouse-design.md`. Each step has a clear "done"
signal so you're never debugging two unverified layers at once. Work through
one step per session with Claude Code, verify it, then move on.

**Before Step 1:** the following architectural decisions are made up front
(see design doc §3 and §7) rather than mid-build, since changing them later
touches multiple layers: windowing = tumbling (5 min), storage backend = local
filesystem (MinIO as stretch), Kafka retention = 72h, and event schema is a
single shared, validated contract.

---

## Step 1: Environment setup
- Install Docker + Docker Compose
- Create project folder structure: `producer/`, `spark_jobs/`, `docker-compose.yml`
- Write `docker-compose.yml` with Kafka + Zookeeper (or Kafka in KRaft mode, no
  Zookeeper needed) services
- `docker-compose up`, confirm Kafka broker is reachable

**Done when:** `docker-compose ps` shows Kafka healthy and reachable on its port.

---

## Step 2: Verify Kafka works standalone
- Create the topic manually:
  `kafka-topics --create --topic clickstream-events --partitions 3 --replication-factor 1`
- Use `kafka-console-producer` and `kafka-console-consumer` to manually send/receive
  a test message
- Confirms Kafka is healthy before any Python or Spark code touches it

**Done when:** a message typed into the console producer appears in the console consumer.

---

## Step 3: Build the Python producer
- Write the event generator (schema + session logic from the design doc)
- Install `kafka-python` or `confluent-kafka`
- Run it, then verify with `kafka-console-consumer` that JSON events are actually
  landing in the topic
- Tune event rate/session count so you have a visible, steady stream

**Done when:** the console consumer shows a continuous stream of well-formed JSON events.

---

## Step 3b: Define and enforce the schema contract
- Extract the event schema into a single shared definition (e.g. `event_schema.py`
  or a JSON Schema file) — not redefined separately in the producer and in Spark
- Validate every producer-generated event against it before publishing to Kafka;
  reject/log anything that doesn't conform
- This exists specifically so a malformed event is caught at the source, not
  silently dropped later by silver's null-filtering (Step 6)

**Done when:** intentionally producing a malformed event (missing field, wrong
type) is caught and rejected by the producer, not passed through to Kafka.

---

## Step 4: Set up local PySpark + Delta
- `pip install pyspark delta-spark`
- **Decide and lock in the storage backend now**: local filesystem for v1
  (MinIO is a stretch goal — see Step 11). This is decided here, not deferred,
  because switching later touches every layer's checkpoint path
- Write a minimal "hello world" Spark session with Delta config enabled
- Write one row to a local Delta path and read it back — confirms Delta is wired
  correctly before adding streaming complexity

**Done when:** a manually written row round-trips through a local Delta table,
and the storage path convention is documented for reuse in every later step.

---

## Step 5: Bronze job — Kafka → Delta
- Write the Spark Structured Streaming job that reads from Kafka, parses JSON,
  writes append-only to `/delta/bronze`
- Run it, let it consume some events, stop it
- Inspect the Delta table with `spark.read.format("delta").load(...)` to confirm
  rows landed correctly
- **Test checkpoint recovery**: kill the job mid-stream, restart it, confirm no
  duplicate/missing offsets

**Done when:** bronze table row count matches events produced, and restart doesn't
duplicate or drop data.

---

## Step 5b: Verify full replay (not just checkpoint-restart)
- Set Kafka topic retention explicitly to 72h at topic creation
  (`retention.ms=259200000`)
- Separately from the Step 5 restart test: delete the bronze Delta table and
  checkpoint entirely, then re-run the bronze job with
  `startingOffsets=earliest` against the still-retained Kafka data
- This tests something different from Step 5 — Step 5 confirms offset resumption
  after a crash; this confirms full historical reprocessing actually works, which
  matters if silver/gold logic changes and bronze needs to be replayed from scratch

**Done when:** a from-scratch bronze rebuild from `earliest` reproduces the same
row count and content as the original run.

---

## Step 6: Silver job — clean and dedupe
- Read bronze as a stream
- Add `event_time` casting, null filtering, `dropDuplicates` with watermark
- Run it against the bronze table, verify silver table row counts make sense
  (should be ≤ bronze due to dedup)

**Done when:** silver table has correct types, no nulls in required fields, and
row count is sane relative to bronze.

---

## Step 7: Gold job — windowed aggregation
- Read silver as a stream
- Implement `withWatermark` + `groupBy(window(...), session_id)` aggregation
- Run it, inspect gold table — check that windows and counts look sane for a few
  known sessions

**Done when:** picking a known session_id, the gold table's counts match what the
producer actually sent for that session.

---

## Step 8: Funnel batch job
- Write a standalone (non-streaming) Spark SQL script that queries gold and
  computes view → cart → purchase conversion
- Run it manually while the gold streaming job is also running, to confirm
  Delta's concurrent read/write isolation actually holds
- **Pass criteria (specific, not just "doesn't error"):**
  1. Row count read by the batch query matches the gold table's committed
     Delta version as of that moment — no partial/torn reads
  2. Re-running the same query after the streaming job commits more data shows
     only additive, consistent changes (no numbers going backwards or duplicating)

**Done when:** both pass criteria above hold across at least 2–3 repeated runs
while the streaming writer is active.

---

## Step 9: Query layer
- Register gold tables (`CREATE TABLE ... USING DELTA LOCATION ...`)
- Write a handful of representative SQL queries (hourly revenue, top products,
  funnel %)
- Optional: wire these into a Streamlit script that re-queries on a timer

**Done when:** you can run each query from a SQL client/notebook and get sensible,
readable output.

---

## Step 9b: Reset/cleanup tooling
- Write a small script (`reset.sh` or similar) that: stops all streaming jobs,
  deletes Delta table directories and checkpoints for bronze/silver/gold, and
  optionally recreates the Kafka topic from scratch
- This matters in practice, not just in theory — you'll be re-running Steps
  5–7 repeatedly while debugging schema/logic changes, and stale checkpoints
  or half-written Delta state will otherwise produce confusing, misleading
  results that look like new bugs

**Done when:** running the reset script and then restarting the pipeline from
Step 5 produces a clean run with no leftover state from prior iterations.

---

## Step 10: End-to-end smoke test
- Start everything fresh: Kafka → producer → bronze → silver → gold, all running
  concurrently
- Let it run for a few minutes, then run the funnel/dashboard queries and
  sanity-check the numbers against what the producer should be generating

**Done when:** the whole pipeline runs unattended for several minutes and gold-layer
numbers are internally consistent with the raw event volume.

---

## Step 11: Stretch features (pick and choose)
- `MERGE INTO` for session upserts instead of pure append
- Schema evolution test (add a field to the producer mid-run, confirm Delta
  handles it via `mergeSchema`)
- `VERSION AS OF` time-travel query for auditability
- `OPTIMIZE` + `ZORDER BY` and measure query time before/after
- Package the whole thing into one `docker-compose up` including Spark

**Done when:** whichever stretch goals you pick behave as expected and you can
explain why they matter in a real production system.
