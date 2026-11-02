"""Deterministic Bronze-to-Silver cleansing transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import PipelineConfig


REQUIRED_COLUMNS = {"order_id", "customer_id", "product_id", "order_timestamp", "quantity", "unit_price"}


def transform_orders(bronze: DataFrame, config: PipelineConfig) -> DataFrame:
    """Standardize order fields, remove duplicates, and calculate net revenue.

    The latest landed version wins for each business key. Invalid required values
    are excluded here; validation metrics are calculated separately before write.
    """
    missing = REQUIRED_COLUMNS - set(bronze.columns)
    if missing:
        raise ValueError(f"Bronze dataset is missing required columns: {sorted(missing)}")
    normalized = bronze.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("product_id").alias("product_id"),
        F.to_timestamp("order_timestamp").alias("order_timestamp"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.coalesce(F.col("discount_amount").cast("decimal(18,2)"), F.lit(0)).alias("discount_amount"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )
    ranked = normalized.withColumn(
        "_row_number", Window.partitionBy(config.primary_key).orderBy(F.col("_ingested_at").desc_nulls_last()).rank()
    )
    return (
        ranked.filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .filter(
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("order_timestamp").isNotNull()
            & (F.col("quantity") > 0)
            & (F.col("unit_price") >= 0)
        )
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        .withColumn("order_date", F.to_date("order_timestamp"))
    )


def write_silver(silver: DataFrame, config: PipelineConfig) -> None:
    """Persist curated orders partitioned by order date."""
    silver.write.format(config.file_format).mode(config.write_mode).partitionBy("order_date").save(config.silver_path)
