"""Quality gates that FAIL the run (ADR-008). Quarantine and warn-level checks come in phase 2."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from es_energy_trade.transforms.aeat import SILVER_KEY


class QualityGateError(RuntimeError):
    pass


def check_silver(df: DataFrame) -> dict[str, int]:
    """Raise on null keys, duplicate keys, or negative weight / value. Returns the counts when all pass."""
    null_keys = df.filter(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in SILVER_KEY])).count()
    duplicate_keys = df.groupBy(*SILVER_KEY).count().filter("count > 1").count()
    negative = df.filter((F.col("weight_kg") < 0) | (F.col("value_eur") < 0)).count()

    problems = {k: v for k, v in
                {"null_keys": null_keys, "duplicate_keys": duplicate_keys, "negative_measures": negative}.items() if v}
    if problems:
        raise QualityGateError(f"Silver batch failed the publishing contract: {problems}")
    return {"rows": df.count(), "null_keys": 0, "duplicate_keys": 0, "negative_measures": 0}
