"""Spark readers for the raw AEAT files. Layout violations fail the run (ADR-008)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from es_energy_trade.transforms.aeat import LEGACY_LINE_LENGTH

N_COLUMNS_2026 = 14
SCHEMA_2026 = StructType([StructField(f"_c{i}", StringType()) for i in range(N_COLUMNS_2026)])


def read_legacy(spark: SparkSession, path: str) -> DataFrame:
    """Read legacy fixed-width lines; raise if any line is not exactly 226 characters."""
    lines = spark.read.text(path).withColumn("_source_file", F.lit(path))
    bad = lines.filter(F.length("value") != LEGACY_LINE_LENGTH)
    n_bad = bad.count()
    if n_bad:
        sample = bad.select(F.length("value").alias("length")).limit(3).collect()
        raise ValueError(
            f"{path}: {n_bad} line(s) with length != {LEGACY_LINE_LENGTH} "
            f"(e.g. {[r.length for r in sample]}). The legacy layout changed."
        )
    return lines


def read_2026(spark: SparkSession, path: str) -> DataFrame:
    """Read the 2026 CSV with an explicit 14-string-column schema.

    FAILFAST: a row with a different number of columns aborts the read instead of being
    padded or truncated silently.
    """
    return (
        spark.read.schema(SCHEMA_2026)
        .option("sep", ";")
        .option("header", "false")
        .option("mode", "FAILFAST")
        .csv(path)
        .withColumn("_source_file", F.lit(path))
    )
