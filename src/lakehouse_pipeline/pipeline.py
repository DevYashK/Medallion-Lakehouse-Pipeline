"""Command-line orchestration for batch execution of all data layers."""

from __future__ import annotations

import argparse
import logging

from .bronze_to_silver import transform_orders, write_silver
from .config import PipelineConfig
from .data_quality import non_negative, non_null, run_quality_gates, unique
from .ingestion import ingest_to_bronze
from .silver_to_gold import write_gold
from .spark import get_spark


def run(source_format: str = "csv") -> None:
    """Execute ingestion, validation, Silver transformation, and Gold modeling."""
    config, spark = PipelineConfig.from_env(), get_spark()
    ingest_to_bronze(spark, config, source_format)
    bronze = spark.read.format(config.file_format).load(config.bronze_path)
    silver = transform_orders(bronze, config)
    run_quality_gates(silver, [non_null("order_id"), unique("order_id"), non_negative("net_amount")])
    write_silver(silver, config)
    write_gold(silver, config)


def main() -> None:
    """Parse command-line options and start a pipeline run."""
    parser = argparse.ArgumentParser(description="Run the medallion batch pipeline.")
    parser.add_argument("--source-format", default="csv", choices=["csv", "json", "parquet"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(args.source_format)


if __name__ == "__main__":
    main()
