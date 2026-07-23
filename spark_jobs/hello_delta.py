"""Step 4 smoke test: confirm PySpark + Delta Lake are wired correctly.

Writes one row to a local Delta table and reads it back. This must pass
before any streaming complexity (Step 5+) is introduced, so a failure
here is unambiguously an environment problem, not a pipeline bug.
"""
from pyspark.sql import Row

from spark_jobs.paths import DELTA_BASE
from spark_jobs.spark_session import get_spark_session

HELLO_WORLD_PATH = f"{DELTA_BASE}/_hello_world"


def main() -> None:
    spark = get_spark_session("hello-delta")

    df = spark.createDataFrame([Row(id=1, message="hello delta lake")])
    df.write.format("delta").mode("overwrite").save(HELLO_WORLD_PATH)

    result = spark.read.format("delta").load(HELLO_WORLD_PATH).collect()

    assert len(result) == 1, f"expected 1 row, got {len(result)}"
    assert result[0]["message"] == "hello delta lake", f"unexpected content: {result[0]}"

    print(f"OK: round-tripped {len(result)} row(s) through {HELLO_WORLD_PATH}")
    print(result)

    spark.stop()


if __name__ == "__main__":
    main()
