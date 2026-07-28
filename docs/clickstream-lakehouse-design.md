# Real-Time E-Commerce Clickstream Analytics — Design Document

## 1. Overview

**Goal:** Build an end-to-end streaming data pipeline that simulates e-commerce user
events, ingests them through Kafka, processes them with Spark Structured Streaming,
and stores progressively refined data in a Delta Lake lakehouse (bronze → silver →
gold). Gold tables are queried with Spark SQL to power funnel and session analytics.

**Why this project:** It exercises the full modern data engineering stack in one
coherent system — event streaming, distributed stream processing, ACID table
storage, and SQL analytics — using patterns (medallion architecture, watermarking,
upserts, schema evolution) that show up constantly in real production systems.

**Out of scope (v1):** authentication/security hardening, multi-tenant isolation,
production-grade orchestration (Airflow/Dagster), cloud deployment. These can be
added in later phases once the core pipeline works locally.

---

## 2. Architecture

```
 ┌─────────────────┐      ┌────────┐      ┌────────────────────────────┐
 │ Python Producer  │ ───▶ │ Kafka  │ ───▶ │ Spark Structured Streaming │
 │ (simulated       │      │ topic: │      │                            │
 │  clickstream)    │      │ clicks │      └──────────────┬─────────────┘
 └─────────────────┘      └────────┘                     │
                                                            ▼
                                                  ┌──────────────────┐
                                                  │  Delta: BRONZE    │  raw, append-only
                                                  │  /delta/bronze    │
                                                  └────────┬─────────┘
                                                            ▼
                                                  ┌──────────────────┐
                                                  │  Delta: SILVER    │  cleaned, deduped,
                                                  │  /delta/silver    │  typed, enriched
                                                  └────────┬─────────┘
                                                            ▼
                                                  ┌──────────────────┐
                                                  │  Delta: GOLD      │  windowed session
                                                  │  /delta/gold      │  metrics, funnels
                                                  └────────┬─────────┘
                                                            ▼
                                                  ┌──────────────────┐
                                                  │  Spark SQL /      │
                                                  │  Dashboard        │
                                                  └──────────────────┘
```

**Data flow pattern:** Medallion architecture (bronze/silver/gold), with each layer
implemented as its own Spark Structured Streaming job reading from the Delta table
of the layer below it (except bronze, which reads from Kafka). Layers are
decoupled — each can be stopped, restarted, or reprocessed independently.

---

## 3. Components

### 3.1 Event Producer (Python)
- Generates synthetic user sessions with realistic event sequences
  (`page_view` → `add_to_cart` → `purchase`, with realistic drop-off).
- Publishes JSON events to a Kafka topic.
- Configurable event rate, session length distribution, and product catalog size.

**Event schema (JSON):**

| Field         | Type     | Notes                                      |
|---------------|----------|---------------------------------------------|
| `event_id`    | string   | UUID, unique per event                      |
| `user_id`     | string   | e.g. `user_123`                             |
| `session_id`  | string   | UUID, shared across a user's session        |
| `event_type`  | string   | `page_view`, `add_to_cart`, `remove_from_cart`, `purchase`, `search` |
| `product_id`  | string   | e.g. `prod_42`                              |
| `price`       | double   | product price at time of event              |
| `timestamp`   | double   | unix epoch seconds, event-time              |

**Schema contract:** This schema is the single source of truth and must be
defined once (e.g. a shared `event_schema.py` / JSON Schema file) and imported
by both the producer and the Spark jobs — not redefined independently in each
place. Producer output is validated against it before publishing (see Build
Sequence, Step 3b), so a malformed event is caught at the source instead of
silently failing or being dropped by silver's null-filtering later.

### 3.2 Kafka
- Single topic `clickstream-events` (v1); revisit partitioning/topic-per-event-type
  if throughput becomes a design concern.
- Partition key: `session_id` (keeps a session's events roughly ordered per
  partition without being a hard requirement for correctness).
- Retention: set explicitly to **72h** (`retention.ms=259200000`) at topic
  creation. This must be long enough to support a full bronze-layer replay from
  earliest offset during development, which is verified explicitly (see Build
  Sequence, Step 5b).

### 3.3 Spark Structured Streaming — Bronze
- Reads raw Kafka messages, parses JSON against the event schema.
- No business logic — append-only landing zone, source of truth for replay.
- Checkpoint location tracks Kafka offsets for exactly-once semantics.

### 3.4 Spark Structured Streaming — Silver
- Reads bronze as a stream.
- Cleans and standardizes: cast `timestamp` → `event_time` (proper timestamp type),
  drop null/malformed rows, `dropDuplicates` on `event_id` (handles Kafka
  at-least-once delivery) with a watermark.
- Optional enrichment: join against a static/reference Delta table (e.g. product
  catalog) if we want product names/categories in later phases.

### 3.5 Spark Structured Streaming — Gold
- Reads silver as a stream.
- Computes windowed session-level aggregates: `page_views`, `cart_adds`,
  `purchases`, `revenue` per `(session_id, time window)`.
- **Windowing strategy (decided): tumbling windows, 5 minutes.** Session windows
  (gap-based, closing a session after N minutes of inactivity) are the more
  semantically "correct" choice for session analytics, but add real complexity
  (gap-duration tuning, session-close detection, more complex watermark
  behavior). v1 uses simple tumbling windows so the pipeline can be built and
  debugged end-to-end first; session windows are a stretch goal once the
  fixed-window version is proven correct.
- Uses `withWatermark` + `groupBy(window(...), session_id)` to handle late data.
- A separate **batch** Spark SQL job computes funnel drop-off (view → cart →
  purchase conversion rates) by querying the gold table — this doesn't need to be
  streaming since it's a periodic rollup.

### 3.6 Spark SQL / Dashboard layer
- Gold Delta tables registered as SQL tables (`CREATE TABLE ... USING DELTA`).
- Ad hoc and scheduled queries: hourly revenue, funnel conversion %, top products,
  session duration distribution.
- v1: query from a notebook / simple script. v2 (stretch): Streamlit dashboard
  polling gold tables on an interval.

---

## 4. Key Design Decisions & Rationale

| Decision | Rationale |
|---|---|
| Medallion architecture (bronze/silver/gold) | Standard pattern; each layer is independently replayable/debuggable; matches real-world lakehouse design |
| Delta Lake over plain Parquet | Need ACID transactions for concurrent streaming writes + batch reads, plus time travel and `MERGE INTO` for upserts |
| Structured Streaming over batch-only | Core learning goal is streaming semantics (watermarks, windowing, checkpointing) |
| `session_id` as grouping key | Enables session-level funnel analysis, the main analytical goal |
| Separate streaming jobs per layer (vs. one big job) | Decoupling — each layer can fail/restart/reprocess independently; mirrors production multi-job pipelines |
| Local-only for v1 (no cloud) | Keep iteration fast; all components run via Docker Compose / local Spark |
| Storage backend: local filesystem for v1, MinIO as explicit stretch goal | Decided once at Step 4, not left open — switching backends later touches every layer's checkpoint path and table location |
| Tumbling windows (5 min) over session windows for v1 | Simpler to build/debug first; session windows are a stretch goal once the fixed-window pipeline is proven correct |
| Kafka retention fixed at 72h, replay explicitly tested | Ensures bronze reprocessing is actually verified, not just assumed to work |
| Shared schema module validated at the producer | Catches malformed events at the source instead of silently failing downstream in silver |

---

## 5. Failure Handling & Edge Cases to Design For

- **Duplicate events** (Kafka at-least-once delivery) → deduped in silver via
  `dropDuplicates(["event_id"])` with watermark.
- **Late-arriving events** → watermarking in gold-layer windowed aggregation;
  decide and document how late is "too late" (events dropped vs. handled).
- **Watermark advancement lags one micro-batch** (verified in Step 7) → the
  watermark used to decide a window is "final" in micro-batch N is computed
  from micro-batch N-1's max event-time, not N's own. In discrete start/stop
  testing this means gold's most recent windows can look "stuck" even after
  feeding in newer data — full convergence needs another later batch to
  actually apply the newly-advanced watermark. Not a bug; just don't expect
  gold's totals to fully reconcile with silver's until the pipeline has run
  continuously for a while (see Step 10).
- **Schema drift** (new fields added to events) → Delta schema evolution
  (`mergeSchema` option); should not break existing streaming jobs.
- **Streaming job restart** → checkpointing must guarantee exactly-once /
  at-least-once processing resumes correctly after a kill/restart.
- **Concurrent batch + streaming access to the same Delta table** → validate
  Delta's ACID isolation holds (e.g. funnel batch job running while gold streaming
  write is in progress). Pass criteria, not just "doesn't error": row count read
  by the batch query at time T matches the gold table's committed version as of
  T (no partial/torn reads), and a second run of the same batch query after the
  streaming job commits more data shows only additive, consistent changes.

---

## 6. Milestones / Build Order

1. **Infra bring-up** — Docker Compose with Kafka + Zookeeper; verify producer can
   publish and a console consumer can read messages.
2. **Producer** — Python script generating realistic sessioned event streams.
3. **Bronze job** — Spark Structured Streaming, Kafka → Delta bronze, verify
   checkpointing/restart behavior.
4. **Silver job** — cleaning, deduplication, type casting.
5. **Gold job** — windowed session aggregation.
6. **Funnel analytics** — batch Spark SQL job against gold.
7. **Query/dashboard layer** — SQL views + simple visualization.
8. **Stretch goals** (pick based on interest/time):
   - `MERGE INTO` for session-state upserts instead of pure append
   - Schema evolution demo (add a field mid-stream)
   - Time travel demo (`VERSION AS OF`) for auditability
   - `OPTIMIZE` + `ZORDER BY` on silver/gold for query performance
   - Streamlit dashboard
   - Migrate storage backend from local filesystem to MinIO (S3-compatible)
   - Containerize the whole pipeline (Spark + Kafka + producer) in one
     `docker-compose up`

---

## 7. Open Questions

**Resolved before build start (see §3 and §4 for rationale):**
- ~~Window type~~ → tumbling windows (5 min) for v1; session windows are a stretch goal.
- ~~Storage backend~~ → local filesystem for v1; MinIO is a stretch goal, decided at Step 4.
- ~~Kafka retention~~ → fixed at 72h, with an explicit replay test (not just checkpoint-restart).
- ~~Schema contract~~ → single shared schema, validated at the producer before publish.

**Still open (resolve opportunistically while building, low architectural risk):**
- Should funnel drop-off eventually be computed as a streaming aggregation
  instead of a batch job, for near-real-time funnel dashboards? (Not required
  for v1 — batch is fine to start.)
- How much event volume/rate should the producer simulate to make windowing and
  late-data handling meaningfully observable? (Tune empirically during Step 3.)

---

## 8. Tech Stack Summary

| Layer | Technology |
|---|---|
| Event generation | Python (`kafka-python` or `confluent-kafka`) |
| Message broker | Apache Kafka (+ Zookeeper or KRaft mode) |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Storage format | Delta Lake |
| Query layer | Spark SQL |
| Orchestration (local) | Docker Compose |
| Dashboard (stretch) | Streamlit |
