"""Landing-zone ingestion into immutable Bronze storage."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig

LOGGER = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    """Raised if a source cannot be safely ingested."""


def read_landing_data(spark: SparkSession, source_path: str, source_format: str = "csv") -> DataFrame:
    """Read a batch from the landing zone with schema inference only at its edge."""
    if not Path(source_path).exists():
        raise IngestionError(f"Landing path does not exist: {source_path}")
    try:
        reader = spark.read.format(source_format).option("header", "true")
        if source_format.lower() == "csv":
            reader = reader.option("inferSchema", "true")
        return reader.load(source_path)
    except Exception as exc:  # Spark preserves the useful JVM exception as cause.
        raise IngestionError(f"Could not read {source_path}: {exc}") from exc


def ingest_to_bronze(spark: SparkSession, config: PipelineConfig, source_format: str = "csv") -> int:
    """Append source records plus lineage columns to the Bronze layer.

    Returns the ingested row count. Bronze data deliberately remains uncleaned
    so that it is auditable and replayable.
    """
    frame = read_landing_data(spark, config.source_path, source_format)
    enriched = (
        frame.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit("batch"))
    )
    try:
        enriched.write.format(config.file_format).mode(config.write_mode).save(config.bronze_path)
        count = enriched.count()
        LOGGER.info("Ingested %s rows into %s", count, config.bronze_path)
        return count
    except Exception as exc:
        raise IngestionError(f"Could not write Bronze data to {config.bronze_path}: {exc}") from exc
