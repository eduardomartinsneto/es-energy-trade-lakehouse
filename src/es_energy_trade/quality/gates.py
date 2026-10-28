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


def check_period_floor(df: DataFrame, floor: str) -> None:
    """Fail if the batch has periods before ``floor`` (ISO date). Keeps the CSV from overwriting legacy history (ADR-015)."""
    early = df.filter(F.col("period") < F.lit(floor).cast("date")).count()
    if early:
        raise QualityGateError(f"{early} row(s) with period before {floor}: the CSV overlaps the legacy history")


def check_release_coverage(df: DataFrame, start: str = "2026-01-01", claimed_through_month: int | None = None):
    """The cumulative CSV must hold every month from ``start`` to its last month, with no gaps.

    This is what proves a file is really cumulative: a release with only the newest month fails here instead of
    quietly loading a fragment. If the release claimed a last month (from its URL), the data must agree.
    Returns the last period found.
    """
    import datetime

    periods = sorted(r.period for r in df.select("period").distinct().collect())
    if not periods:
        raise QualityGateError("The release has no rows")
    first = datetime.date.fromisoformat(start)
    last = periods[-1]
    expected, (y, m) = [], (first.year, first.month)
    while (y, m) <= (last.year, last.month):
        expected.append(datetime.date(y, m, 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    missing = [p.isoformat() for p in expected if p not in periods]
    if missing:
        raise QualityGateError(f"Not cumulative from {start}: missing months {missing}")
    if claimed_through_month is not None and last.month != claimed_through_month:
        raise QualityGateError(f"The release claims to end in month {claimed_through_month} but the data ends in {last}")
    return last
