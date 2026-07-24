"""Shared SparkSession builder with Delta Lake configured.

Every Spark job (bronze/silver/gold, Steps 5-7) should build its session
through get_spark_session() rather than repeating the Delta extension and
catalog config in each job file.
"""
from typing import List, Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, extra_packages: Optional[List[str]] = None) -> SparkSession:
    """Build a Delta-configured SparkSession.

    extra_packages is for job-specific Maven coordinates (e.g. bronze needs
    the Kafka connector; silver/gold read only from Delta and don't).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
