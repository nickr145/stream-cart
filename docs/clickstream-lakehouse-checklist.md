# Clickstream Lakehouse — Build Checklist

Tracks progress through `clickstream-lakehouse-build-steps.md`. Check off a step
once its "done when" criterion is verified.

- [x] Step 1: Environment setup
- [x] Step 2: Verify Kafka works standalone
- [x] Step 3: Build the Python producer
- [x] Step 3b: Define and enforce the schema contract
- [x] Step 4: Set up local PySpark + Delta
- [x] Step 5: Bronze job — Kafka → Delta
- [x] Step 5b: Verify full replay (not just checkpoint-restart)
- [x] Step 6: Silver job — clean and dedupe
- [ ] Step 7: Gold job — windowed aggregation
- [ ] Step 8: Funnel batch job
- [ ] Step 9: Query layer
- [ ] Step 9b: Reset/cleanup tooling
- [ ] Step 10: End-to-end smoke test
- [ ] Step 11: Stretch features (pick and choose)
