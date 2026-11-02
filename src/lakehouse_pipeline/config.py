"""Typed configuration and safe environment overrides for the pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(ValueError):
    """Raised when a runtime configuration value is invalid."""


@dataclass(frozen=True)
class PipelineConfig:
    """Locations and write behaviour used by all pipeline stages.

    Environment variables take precedence over defaults, allowing the same code
    to run locally and in orchestration platforms without source changes.
    """

    source_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    checkpoint_path: str
    file_format: str = "parquet"
    write_mode: str = "append"
    primary_key: str = "order_id"
    event_timestamp_column: str = "order_timestamp"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Build configuration from environment variables with local defaults."""
        root = Path(os.getenv("DATA_ROOT", "data"))
        config = cls(
            source_path=os.getenv("SOURCE_PATH", str(root / "landing" / "orders")),
            bronze_path=os.getenv("BRONZE_PATH", str(root / "bronze" / "orders")),
            silver_path=os.getenv("SILVER_PATH", str(root / "silver" / "orders")),
            gold_path=os.getenv("GOLD_PATH", str(root / "gold")),
            checkpoint_path=os.getenv("CHECKPOINT_PATH", str(root / "checkpoints")),
            file_format=os.getenv("FILE_FORMAT", "parquet").lower(),
            write_mode=os.getenv("WRITE_MODE", "append").lower(),
            primary_key=os.getenv("PRIMARY_KEY", "order_id"),
            event_timestamp_column=os.getenv("EVENT_TIMESTAMP_COLUMN", "order_timestamp"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Fail early for values that would produce unsafe Spark writes."""
        if self.file_format not in {"parquet", "delta"}:
            raise ConfigurationError("FILE_FORMAT must be 'parquet' or 'delta'.")
        if self.write_mode not in {"append", "overwrite"}:
            raise ConfigurationError("WRITE_MODE must be 'append' or 'overwrite'.")
        if not self.primary_key.strip() or not self.event_timestamp_column.strip():
            raise ConfigurationError("Primary key and timestamp column must be non-empty.")
