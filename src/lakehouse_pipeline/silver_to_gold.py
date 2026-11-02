"""Silver-to-Gold star-schema construction for order analytics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import PipelineConfig


def build_dim_date(silver: DataFrame) -> DataFrame:
    """Create a conformed calendar dimension from observed order dates."""
    return silver.select("order_date").distinct().select(
        F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("order_date").alias("full_date"),
        F.year("order_date").alias("year"), F.quarter("order_date").alias("quarter"),
        F.month("order_date").alias("month"), F.date_format("order_date", "MMMM").alias("month_name"),
        F.dayofmonth("order_date").alias("day_of_month"), F.date_format("order_date", "EEEE").alias("day_name"),
    )


def build_dim_customer(silver: DataFrame) -> DataFrame:
    """Build a customer dimension from the latest customer occurrence."""
    return silver.select("customer_id").where(F.col("customer_id").isNotNull()).distinct().withColumn("customer_key", F.xxhash64("customer_id"))


def build_dim_product(silver: DataFrame) -> DataFrame:
    """Build a product dimension from observed product identifiers."""
    return silver.select("product_id").where(F.col("product_id").isNotNull()).distinct().withColumn("product_key", F.xxhash64("product_id"))


def build_fact_sales(silver: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Create the order-line fact table with surrogate dimension keys."""
    return (
        silver.join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .select(
            F.col("order_id"), F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
            "customer_key", "product_key", "quantity", "unit_price", "discount_amount", "gross_amount", "net_amount",
        )
    )


def write_gold(silver: DataFrame, config: PipelineConfig) -> None:
    """Materialize all Gold dimensions and fact tables under the Gold root."""
    customers, products = build_dim_customer(silver), build_dim_product(silver)
    tables = {"dim_date": build_dim_date(silver), "dim_customer": customers, "dim_product": products, "fact_sales": build_fact_sales(silver, customers, products)}
    for name, frame in tables.items():
        frame.write.format(config.file_format).mode(config.write_mode).save(f"{config.gold_path}/{name}")
