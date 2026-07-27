"""Step 6: Silver job -- clean, dedupe, and type the bronze stream.

Reads bronze as a stream and produces the cleaned business-level event
stream (design doc SS3.4): drops rows missing a required field or carrying
an invalid event_type (bronze deliberately does no such filtering, see
spark_jobs/bronze.py), casts the raw epoch-seconds timestamp to a proper
event_time, and deduplicates on event_id (Kafka at-least-once delivery)
within a watermark window.
"""
from pyspark.sql.functions import col

from schemas.event_schema import EVENT_TYPES, FIELD_NAMES
from spark_jobs.paths import CHECKPOINT_SILVER, DELTA_BRONZE, DELTA_SILVER
from spark_jobs.spark_session import get_spark_session

# Bounds how long dedup state is retained past the latest seen event_time.
# Generous relative to this pipeline's event pacing (seconds, not hours).
WATERMARK_DELAY = "10 minutes"


def main() -> None:
    spark = get_spark_session("silver")
    spark.sparkContext.setLogLevel("WARN")

    bronze_stream = spark.readStream.format("delta").load(DELTA_BRONZE)

    cleaned = (
        bronze_stream.na.drop(subset=FIELD_NAMES)
        .filter(col("event_type").isin(EVENT_TYPES))
        .withColumn("event_time", col("timestamp").cast("timestamp"))
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
        .select(*FIELD_NAMES, "event_time")
    )

    query = (
        cleaned.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .start(DELTA_SILVER)
    )

    print(f"Silver job running. Writing to {DELTA_SILVER}, checkpoint at {CHECKPOINT_SILVER}.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
