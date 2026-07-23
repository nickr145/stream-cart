"""Step 5: Bronze job -- Kafka -> Delta, append-only, no business logic.

Raw landing zone / source of truth for replay (design doc SS3.3): keeps the
raw Kafka value alongside the parsed event fields, using a nullable
version of the shared schema so a malformed message is preserved, not
dropped or crash the job -- filtering bad rows is silver's job (Step 6).
"""
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType

from schemas.event_schema import to_spark_struct_type
from spark_jobs.paths import CHECKPOINT_BRONZE, DELTA_BRONZE
from spark_jobs.spark_session import get_spark_session

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "clickstream-events"
SPARK_VERSION = "3.5.9"
KAFKA_CONNECTOR_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def nullable_event_schema() -> StructType:
    """Same fields as the shared schema, but nullable.

    A NOT NULL schema would make Delta reject any row parsed from a
    genuinely malformed message; bronze must preserve it instead.
    """
    strict = to_spark_struct_type()
    return StructType([StructField(f.name, f.dataType, nullable=True) for f in strict.fields])


def main() -> None:
    spark = get_spark_session("bronze", extra_packages=[KAFKA_CONNECTOR_PACKAGE])
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = nullable_event_schema()
    raw_value = col("value").cast("string")

    parsed = kafka_stream.select(
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        raw_value.alias("raw_value"),
        from_json(raw_value, schema).alias("event"),
    ).select("kafka_partition", "kafka_offset", "kafka_timestamp", "raw_value", "event.*")

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .start(DELTA_BRONZE)
    )

    print(f"Bronze job running. Writing to {DELTA_BRONZE}, checkpoint at {CHECKPOINT_BRONZE}.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
