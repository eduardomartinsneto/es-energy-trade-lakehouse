"""Gold tables for the business question (ADR-008). All business logic lives here, never in DAX."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Mirrors ``products`` in config/sources.yml (CN prefixes). The notebook passes the YAML values in.
DEFAULT_PRODUCTS = {"crude_oil": ["2709"], "lng": ["271111"], "pipeline_gas": ["271121"]}

# Below this monthly volume per origin the implied price is not reported (ADR-016): tiny, often
# mis-declared flows give prices of millions of EUR/t. Weight, value and shares are unaffected.
MIN_WEIGHT_FOR_PRICE_KG = 1_000_000  # 1,000 t


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
    product = F.lit(None).cast("string")
    for name, prefixes in products.items():
        cond = F.lit(False)
        for prefix in prefixes:
            cond = cond | F.col("cn8").startswith(prefix)
        product = F.when(cond & product.isNull(), F.lit(name)).otherwise(product)

    grouped = (
        silver.filter(F.col("flow") == "M")
        .withColumn("product", product)
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


def supply_concentration(imports_monthly: DataFrame) -> DataFrame:
    """Grain: month x product. HHI of origins on the 0-10,000 scale (by weight), top-1 share, number of origins."""
    return imports_monthly.filter(F.col("weight_kg") > 0).groupBy("period", "product").agg(
        F.round(F.sum(F.col("share_of_weight") ** 2) * 10000, 1).alias("hhi"),
        F.max("share_of_weight").alias("top1_share"),
        F.count("*").alias("n_origins"),
        F.sum("weight_kg").alias("total_weight_kg"),
        F.sum("value_eur").alias("total_value_eur"),
    )
