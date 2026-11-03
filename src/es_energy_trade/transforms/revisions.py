"""Revision tracking for the Silver MERGE (ADR-007): which existing rows would change, and how."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from es_energy_trade.transforms.aeat import SILVER_KEY

MEASURES = ["weight_kg", "value_eur", "supp_units"]


def changed_rows(source: DataFrame, target: DataFrame) -> DataFrame:
    """Rows present in both whose measures differ (different ``_row_hash``): old and new values side by side.

    Call it BEFORE the MERGE, while ``target`` still holds the old values. Keys that appear in only one side
    are not revisions (new rows are inserts). Keys that disappear from a republished file are `vanished_rows`.
    """
    old = target.select(
        *SILVER_KEY,
        *[F.col(m).alias(f"{m}_old") for m in MEASURES],
        F.col("_row_hash").alias("_row_hash_old"),
    )
    new = source.select(
        *SILVER_KEY,
        *[F.col(m).alias(f"{m}_new") for m in MEASURES],
        F.col("_row_hash").alias("_row_hash_new"),
        "_source_file",
    )
    out = new.join(old, SILVER_KEY, "inner").filter(F.col("_row_hash_old") != F.col("_row_hash_new"))
    return out.withColumn("_change_type", F.lit("revised"))


def vanished_rows(source: DataFrame, target: DataFrame, source_format: str = "2026") -> DataFrame:
    """Target rows of one source format whose key is absent from ``source`` (a full re-delivery of that format).

    ADR-017: the CSV is the source of truth for 2026, so a key the latest release no longer contains is removed from
    Silver. This records it first, in the same shape as ``changed_rows`` (``*_new`` columns are NULL).
    Only valid when ``source`` is a complete release of ``source_format``; never call it with a partial legacy batch.
    """
    gone = target.filter(F.col("source_format") == source_format).join(source.select(*SILVER_KEY), SILVER_KEY, "left_anti")
    new_nulls = [F.lit(None).cast(dict(target.dtypes)[m]).alias(f"{m}_new") for m in MEASURES]
    return gone.select(
        *SILVER_KEY,
        *new_nulls,
        F.lit(None).cast("string").alias("_row_hash_new"),
        F.lit(None).cast("string").alias("_source_file"),
        *[F.col(m).alias(f"{m}_old") for m in MEASURES],
        F.col("_row_hash").alias("_row_hash_old"),
        F.lit("vanished").alias("_change_type"),
    )
