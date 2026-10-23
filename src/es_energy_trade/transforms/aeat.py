"""Parse AEAT "Máxima desagregación" files into the conformed Silver schema.

Two input formats (see docs/decisions.md, ADR-007 / ADR-011 / ADR-012 and the Phase 0 findings):

* legacy  - fixed width, 226 chars, one row per customs declaration.
            Weight and supplementary units carry 3 implied decimals, value is in euro cents.
* 2026    - ';' separated, 14 columns, already aggregated. Weight in kg, value in euros.

Everything here is a pure DataFrame -> DataFrame function: no I/O, no side effects.
Callers must provide a ``_source_file`` column (``_metadata.file_path`` on Databricks).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LEGACY_LINE_LENGTH = 226
ENERGY_CHAPTER = "27"
# The 2026 file has chapter-27 rows with no transport mode (low-value shipments under the residual
# CN 27000000, some exports to UZ). MERGE keys must be non-null, so these get an explicit sentinel (ADR-014).
MISSING_TRANSPORT_MODE = "NA"

# Silver MERGE key (ADR-012). ``source_format`` is deliberately part of the aggregation
# grain: if both formats ever reach the same batch for one key, the duplicate-key gate
# must fail the run instead of silently summing the same trade twice (ADR-007).
SILVER_KEY = ["flow", "period", "reporter_iso2", "cn8", "partner_code_src", "transport_mode"]

# 1-based (start, length) positions, from data/samples/diseno226.pdf
_LEGACY_FIELDS = {
    "flow": (1, 1),
    "year": (2, 2),
    "month": (4, 2),
    "cn8": (26, 8),
    "partner": (67, 3),  # country of origin / destination
    "weight": (90, 15),
    "units": (105, 15),
    "value": (120, 12),
    "transport_mode": (165, 1),  # mode at the border
}

# 0-based column index in the 14-column 2026 CSV (sheet "Diseño de registro")
_C2026 = {
    "flow": "_c0",
    "year": "_c1",
    "month": "_c2",
    "cn": "_c3",
    "partner": "_c4",
    "weight": "_c6",
    "units": "_c7",
    "value": "_c8",
    "transport_mode": "_c10",
}

# AEAT flow letters: I = import, E = export (verified on the Jan-2025 legacy file and the 2026 CSV).
_FLOW = {"I": "M", "E": "X"}


def _clean(col: Column) -> Column:
    """Trim; empty string -> NULL so that missing keys are caught by the null-key gate."""
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _flow(col: Column) -> Column:
    expr = F.lit(None).cast("string")
    for src, dst in _FLOW.items():
        expr = F.when(col == src, F.lit(dst)).otherwise(expr)
    return expr


def _legacy(name: str, line: Column) -> Column:
    start, length = _LEGACY_FIELDS[name]
    return F.substring(line, start, length)


def parse_legacy(lines: DataFrame, line_col: str = "value") -> DataFrame:
    """Fixed-width legacy lines -> declaration-level rows in the conformed schema."""
    line = F.col(line_col)
    return lines.select(
        _flow(_legacy("flow", line)).alias("flow"),
        F.make_date(
            F.lit(2000) + _legacy("year", line).cast("int"),
            _legacy("month", line).cast("int"),
            F.lit(1),
        ).alias("period"),
        F.lit("ES").alias("reporter_iso2"),
        _clean(_legacy("cn8", line)).alias("cn8"),
        _clean(F.upper(_legacy("partner", line))).alias("partner_code_src"),
        F.coalesce(_clean(_legacy("transport_mode", line)), F.lit(MISSING_TRANSPORT_MODE)).alias("transport_mode"),
        (_legacy("weight", line).cast("decimal(18,0)") / 1000).cast("decimal(18,3)").alias("weight_kg"),
        (_legacy("units", line).cast("decimal(18,0)") / 1000).cast("decimal(18,3)").alias("supp_units"),
        (_legacy("value", line).cast("decimal(18,0)") / 100).cast("decimal(18,2)").alias("value_eur"),
        F.lit("legacy").alias("source_format"),
        F.col("_source_file"),
    )


def parse_2026(raw: DataFrame) -> DataFrame:
    """2026 CSV rows (columns ``_c0`` .. ``_c13``, all strings) -> aggregated rows, conformed schema."""
    c = {name: F.col(col) for name, col in _C2026.items()}
    return raw.select(
        _flow(F.trim(c["flow"])).alias("flow"),
        F.make_date(c["year"].cast("int"), c["month"].cast("int"), F.lit(1)).alias("period"),
        F.lit("ES").alias("reporter_iso2"),
        # the 2026 file mixes 8- and 10-digit CN codes; the Silver grain is CN8
        _clean(F.substring(c["cn"], 1, 8)).alias("cn8"),
        _clean(F.upper(c["partner"])).alias("partner_code_src"),
        F.coalesce(_clean(c["transport_mode"]), F.lit(MISSING_TRANSPORT_MODE)).alias("transport_mode"),
        c["weight"].cast("decimal(18,3)").alias("weight_kg"),
        c["units"].cast("decimal(18,3)").alias("supp_units"),
        c["value"].cast("decimal(18,2)").alias("value_eur"),
        F.lit("2026").alias("source_format"),
        F.col("_source_file"),
    )


def to_silver(parsed: DataFrame, chapter: str = ENERGY_CHAPTER) -> DataFrame:
    """Keep one CN chapter (ADR-011) and aggregate to the Silver grain (ADR-012).

    ``_row_hash`` covers the measures only, so re-delivered identical data is not a revision (ADR-007).
    """
    agg = (
        parsed.filter(F.col("cn8").startswith(chapter))
        .groupBy(*SILVER_KEY, "source_format")
        .agg(
            F.sum("weight_kg").cast("decimal(18,3)").alias("weight_kg"),
            F.sum("value_eur").cast("decimal(18,2)").alias("value_eur"),
            F.sum("supp_units").cast("decimal(18,3)").alias("supp_units"),
            F.max("_source_file").alias("_source_file"),
        )
    )
    return agg.withColumn(
        "_row_hash",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("weight_kg").cast("string"),
                F.col("value_eur").cast("string"),
                F.col("supp_units").cast("string"),
            ),
            256,
        ),
    )
