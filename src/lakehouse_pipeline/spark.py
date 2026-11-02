"""Spark session construction with consistent lakehouse settings."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "medallion-data-platform") -> SparkSession:
    """Return an existing or local Spark session with a consistent UTC timezone.

    Parquet works without extra dependencies. If Delta is selected, the
    ``delta-spark`` package must also be available on the Spark classpath.
    """
    return SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()
