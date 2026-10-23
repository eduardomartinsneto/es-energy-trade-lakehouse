"""Revision tracking for the Silver MERGE (ADR-007): which existing rows would change, and how."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from es_energy_trade.transforms.aeat import SILVER_KEY

MEASURES = ["weight_kg", "value_eur", "supp_units"]


def changed_rows(source: DataFrame, target: DataFrame) -> DataFrame:
    """Rows present in both whose measures differ (different ``_row_hash``): old and new values side by side.

    Call it BEFORE the MERGE, while ``target`` still holds the old values. Keys that appear in only one side
    are not revisions (new rows are inserts). A key that disappears from a republished file is not detected here.
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
    return new.join(old, SILVER_KEY, "inner").filter(F.col("_row_hash_old") != F.col("_row_hash_new"))
