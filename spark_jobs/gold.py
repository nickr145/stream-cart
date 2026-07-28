"""Step 7: Gold job -- windowed session-level aggregation.

Reads silver as a stream and computes per-(session_id, 5-minute tumbling
window) metrics (design doc SS3.5): page_views, cart_adds, purchases,
revenue. Tumbling windows chosen over session (gap-based) windows for v1
simplicity -- see design doc SS3.5 / SS4 for the full rationale.
"""
from pyspark.sql import functions as F

from spark_jobs.paths import CHECKPOINT_GOLD, DELTA_GOLD, DELTA_SILVER
from spark_jobs.spark_session import get_spark_session

WINDOW_DURATION = "5 minutes"

# Bounds how long a window stays open waiting for late-arriving events
# before being finalized and emitted (append mode only emits a window
# once the watermark passes its end). Generous relative to this
# pipeline's actual event pacing (seconds, not hours).
WATERMARK_DELAY = "2 minutes"


def main() -> None:
    spark = get_spark_session("gold")
    spark.sparkContext.setLogLevel("WARN")

    silver_stream = spark.readStream.format("delta").load(DELTA_SILVER)

    gold = (
        silver_stream.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(F.window("event_time", WINDOW_DURATION), F.col("session_id"))
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)
            ).alias("revenue"),
        )
        .select(
            F.col("session_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "page_views",
            "cart_adds",
            "purchases",
            "revenue",
        )
    )

    query = (
        gold.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_GOLD)
        .start(DELTA_GOLD)
    )

    print(f"Gold job running. Writing to {DELTA_GOLD}, checkpoint at {CHECKPOINT_GOLD}.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
