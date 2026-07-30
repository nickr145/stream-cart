"""Step 8: Funnel batch job -- view -> cart -> purchase conversion.

Standalone (non-streaming) Spark SQL query against the gold table,
computing session-level funnel drop-off (design doc SS3.5). Doesn't need
to be streaming since it's a periodic rollup.

Pins the read to gold's current committed Delta version via versionAsOf
rather than a plain load(), so the version actually read is known and
reproducible -- this is what lets a caller (see the Step 8 concurrency
test) prove the read is a consistent snapshot even while gold's
streaming writer is committing new data concurrently, instead of just
hoping nothing raced.

A session's activity is aggregated across any gold window rows it spans
(see design doc SS5's windowing note) before computing funnel membership,
so a session split across window boundaries isn't undercounted.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_jobs.paths import DELTA_GOLD
from spark_jobs.spark_session import get_spark_session


def compute_funnel(spark) -> dict:
    version = DeltaTable.forPath(spark, DELTA_GOLD).history(1).select("version").first()["version"]
    gold = spark.read.format("delta").option("versionAsOf", version).load(DELTA_GOLD)

    per_session = gold.groupBy("session_id").agg(
        F.sum("page_views").alias("page_views"),
        F.sum("cart_adds").alias("cart_adds"),
        F.sum("purchases").alias("purchases"),
    )

    row = per_session.agg(
        F.sum((F.col("page_views") > 0).cast("int")).alias("viewed"),
        F.sum((F.col("cart_adds") > 0).cast("int")).alias("carted"),
        F.sum((F.col("purchases") > 0).cast("int")).alias("purchased"),
    ).first()

    viewed = row["viewed"] or 0
    carted = row["carted"] or 0
    purchased = row["purchased"] or 0

    return {
        "gold_version": version,
        "viewed_sessions": viewed,
        "carted_sessions": carted,
        "purchased_sessions": purchased,
        "view_to_cart_rate": (carted / viewed) if viewed else 0.0,
        "cart_to_purchase_rate": (purchased / carted) if carted else 0.0,
        "view_to_purchase_rate": (purchased / viewed) if viewed else 0.0,
    }


def main() -> None:
    spark = get_spark_session("funnel")
    spark.sparkContext.setLogLevel("WARN")

    result = compute_funnel(spark)
    for key, value in result.items():
        print(f"{key}={value}")

    spark.stop()


if __name__ == "__main__":
    main()
