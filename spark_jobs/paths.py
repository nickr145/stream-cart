"""Local filesystem storage path conventions for the Delta lakehouse.

Storage backend decided at Step 4: local filesystem for v1 (MinIO is a
stretch goal, see design doc SS4 / build-steps Step 11). Locked in here so
switching backends later means editing this one file, not every job.

All paths are relative to the repo root. Spark jobs should be run from
the repo root with PYTHONPATH=. (matching the convention established for
the producer in Step 3b) so these resolve consistently.
"""
import os

DELTA_BASE = "delta"
CHECKPOINT_BASE = "checkpoints"

DELTA_BRONZE = os.path.join(DELTA_BASE, "bronze")
DELTA_SILVER = os.path.join(DELTA_BASE, "silver")
DELTA_GOLD = os.path.join(DELTA_BASE, "gold")

CHECKPOINT_BRONZE = os.path.join(CHECKPOINT_BASE, "bronze")
CHECKPOINT_SILVER = os.path.join(CHECKPOINT_BASE, "silver")
CHECKPOINT_GOLD = os.path.join(CHECKPOINT_BASE, "gold")
