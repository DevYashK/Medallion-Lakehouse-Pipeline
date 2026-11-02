"""Reusable, fail-fast data quality gates for Spark DataFrames."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(RuntimeError):
    """Raised when one or more required quality gates fail."""


@dataclass(frozen=True)
class QualityResult:
    """Outcome of one quality check, suitable for logs or audit storage."""

    name: str
    passed: bool
    observed: int
    threshold: int


QualityCheck = Callable[[DataFrame], QualityResult]


def non_null(column: str) -> QualityCheck:
    """Return a check requiring no null or blank values in a column."""
    def check(frame: DataFrame) -> QualityResult:
        failures = frame.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")).count()
        return QualityResult(f"{column}_non_null", failures == 0, failures, 0)
    return check


def unique(column: str) -> QualityCheck:
    """Return a check requiring a unique business key."""
    def check(frame: DataFrame) -> QualityResult:
        duplicates = frame.groupBy(column).count().filter(F.col("count") > 1).count()
        return QualityResult(f"{column}_unique", duplicates == 0, duplicates, 0)
    return check


def non_negative(column: str) -> QualityCheck:
    """Return a check requiring values to be zero or greater."""
    def check(frame: DataFrame) -> QualityResult:
        failures = frame.filter(F.col(column).isNull() | (F.col(column) < 0)).count()
        return QualityResult(f"{column}_non_negative", failures == 0, failures, 0)
    return check


def run_quality_gates(frame: DataFrame, checks: list[QualityCheck]) -> list[QualityResult]:
    """Run every check and raise once with all failed gate names."""
    results = [check(frame) for check in checks]
    failed = [result for result in results if not result.passed]
    if failed:
        detail = ", ".join(f"{item.name} ({item.observed} failures)" for item in failed)
        raise DataQualityError(f"Data quality gates failed: {detail}")
    return results
