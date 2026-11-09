"""Gold tables for the business question (ADR-008). All business logic lives here, never in DAX."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Mirrors ``products`` in config/sources.yml (CN prefixes). The notebook passes the YAML values in.
DEFAULT_PRODUCTS = {"crude_oil": ["2709"], "lng": ["271111"], "pipeline_gas": ["271121"]}

# Below this monthly volume per origin the implied price is not reported (ADR-016): tiny, often
# mis-declared flows give prices of millions of EUR/t. Weight, value and shares are unaffected.
MIN_WEIGHT_FOR_PRICE_KG = 1_000_000  # 1,000 t


def _product_column(products: dict[str, list[str]]):
    """Project product from the CN8 code (first matching CN prefix wins); NULL when it matches none."""
    product = F.lit(None).cast("string")
    for name, prefixes in products.items():
        cond = F.lit(False)
        for prefix in prefixes:
            cond = cond | F.col("cn8").startswith(prefix)
        product = F.when(cond & product.isNull(), F.lit(name)).otherwise(product)
    return product


def energy_imports_monthly(
    silver: DataFrame,
    products: dict[str, list[str]] = DEFAULT_PRODUCTS,
    min_weight_for_price_kg: int = MIN_WEIGHT_FOR_PRICE_KG,
) -> DataFrame:
    """Grain: month x product x origin. Volume, value, implied price (EUR/t) and share of the month's volume.

    Transport modes are summed. Residual CN codes such as 27000000 match no product and are dropped.
    Origin is the AEAT country code as delivered (for LNG it may be provenance, see ADR-008 limitations).
    ``implied_price_eur_per_t`` is NULL when the origin-month weight is below ``min_weight_for_price_kg`` (ADR-016).
    """
    grouped = (
        silver.filter(F.col("flow") == "M")
        .withColumn("product", _product_column(products))
        .filter(F.col("product").isNotNull())
        .groupBy("period", "product", F.col("partner_code_src").alias("origin_code"))
        .agg(F.sum("weight_kg").alias("weight_kg"), F.sum("value_eur").alias("value_eur"))
    )
    by_month = Window.partitionBy("period", "product")
    return grouped.select(
        "period", "product", "origin_code", "weight_kg", "value_eur",
        F.when(F.col("weight_kg") >= min_weight_for_price_kg, (F.col("value_eur") / (F.col("weight_kg") / 1000)).cast("decimal(18,2)"))
        .alias("implied_price_eur_per_t"),
        F.when(F.sum("weight_kg").over(by_month) > 0,
               (F.col("weight_kg") / F.sum("weight_kg").over(by_month)).cast("double")).alias("share_of_weight"),
    )


def supply_concentration(
    imports_monthly: DataFrame, min_weight_for_price_kg: int = MIN_WEIGHT_FOR_PRICE_KG
) -> DataFrame:
    """Grain: month x product. HHI of origins on the 0-10,000 scale (by weight), top-1 share, number of origins.

    ``implied_price_eur_per_t`` is the product-level price (total value / total tonnes over all origins), so reports
    never average the origin-level price. NULL below ``min_weight_for_price_kg``, the same rule as ADR-016.
    """
    agg = imports_monthly.filter(F.col("weight_kg") > 0).groupBy("period", "product").agg(
        F.round(F.sum(F.col("share_of_weight") ** 2) * 10000, 1).alias("hhi"),
        F.max("share_of_weight").alias("top1_share"),
        F.count("*").alias("n_origins"),
        F.sum("weight_kg").alias("total_weight_kg"),
        F.sum("value_eur").alias("total_value_eur"),
    )
    return agg.withColumn(
        "implied_price_eur_per_t",
        F.when(F.col("total_weight_kg") >= min_weight_for_price_kg,
               (F.col("total_value_eur") / (F.col("total_weight_kg") / 1000)).cast("decimal(18,2)")),
    )


def revisions_summary(
    revisions: DataFrame,
    products: dict[str, list[str]] = DEFAULT_PRODUCTS,
    silver: DataFrame | None = None,
) -> DataFrame:
    """Grain: run x reference month x flow x product ('other' for CN codes outside the project products).

    Summarises the Silver revisions log: how much already-published official data changed in each run.
    A *revised* row has new and old measures; a *vanished* row (ADR-017) has no new measures, so its delta is
    ``0 - old``. Rows logged before ``_change_type`` existed count as revised.
    ``months_after_period`` is the lag between the date of the run and the reference month (0 = the run's own month).
    It measures when the pipeline ran, not when AEAT published the release, so it is only an upper bound on the
    age of the data at publication.
    ``abs_value_delta_share_of_month`` divides the absolute value change by the month's value in ``silver`` (the
    current table, i.e. after the run), so it is a size, not an exact percentage; NULL when ``silver`` is not given.
    """
    rev = revisions if "_change_type" in revisions.columns else revisions.withColumn("_change_type", F.lit("revised"))
    rev = rev.withColumn("_change_type", F.coalesce(F.col("_change_type"), F.lit("revised")))
    value_delta = F.coalesce(F.col("value_eur_new"), F.lit(0)) - F.col("value_eur_old")
    weight_delta = F.coalesce(F.col("weight_kg_new"), F.lit(0)) - F.col("weight_kg_old")
    keyed = (
        rev.withColumn("product", F.coalesce(_product_column(products), F.lit("other")))
        .withColumn("_value_delta", value_delta)
        .withColumn("_weight_delta", weight_delta)
    )
    out = keyed.groupBy(F.col("_run_at").alias("run_at"), "period", "flow", "product").agg(
        F.sum(F.when(F.col("_change_type") == "revised", 1).otherwise(0)).alias("n_revised"),
        F.sum(F.when(F.col("_change_type") == "vanished", 1).otherwise(0)).alias("n_vanished"),
        F.sum("value_eur_old").alias("value_old_eur"),
        F.sum("_value_delta").alias("value_delta_eur"),
        F.sum(F.abs("_value_delta")).alias("abs_value_delta_eur"),
        F.sum("weight_kg_old").alias("weight_old_kg"),
        F.sum("_weight_delta").alias("weight_delta_kg"),
    ).withColumn(
        "months_after_period",
        F.months_between(F.trunc(F.col("run_at").cast("date"), "month"), F.col("period")).cast("int"),
    )
    if silver is None:
        out = out.withColumn("period_value_eur", F.lit(None).cast("decimal(18,2)"))
    else:
        month = (
            silver.withColumn("product", F.coalesce(_product_column(products), F.lit("other")))
            .groupBy("period", "flow", "product")
            .agg(F.sum("value_eur").alias("period_value_eur"))
        )
        out = out.join(month, ["period", "flow", "product"], "left")
    return out.withColumn(
        "abs_value_delta_share_of_month",
        F.when(F.col("period_value_eur") > 0, (F.col("abs_value_delta_eur") / F.col("period_value_eur")).cast("double")),
    ).select(
        "run_at", "period", "flow", "product", "months_after_period", "n_revised", "n_vanished",
        "value_old_eur", "value_delta_eur", "abs_value_delta_eur", "period_value_eur",
        "abs_value_delta_share_of_month", "weight_old_kg", "weight_delta_kg",
    )
