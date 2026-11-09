"""Build ``revisions_summary`` from local exports, without Databricks (the Gold logic is the same function).

Inputs  (not in git):  data/exports/trade_flows_aeat_revisions.csv   revisions log exported from Databricks
                       data/silver_offline/trade_flows_aeat.csv       current Silver (denominator of the share)
Output  (not in git):  data/gold_offline/revisions_summary.csv

Run from the repository root:  python scripts/build_revisions_summary_offline.py
"""
from __future__ import annotations

import pathlib
import sys

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from es_energy_trade.transforms.gold import revisions_summary  # noqa: E402

REVISIONS = ROOT / "data" / "exports" / "trade_flows_aeat_revisions.csv"
SILVER = ROOT / "data" / "silver_offline" / "trade_flows_aeat.csv"
OUT = ROOT / "data" / "gold_offline" / "revisions_summary.csv"

DECIMALS = {"weight_kg": "decimal(18,3)", "value_eur": "decimal(18,2)", "supp_units": "decimal(18,3)"}


def main() -> None:
    spark = (SparkSession.builder.master("local[2]").appName("revisions-summary-offline")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    products = yaml.safe_load((ROOT / "config" / "sources.yml").read_text())["products"]

    rev = spark.read.csv(str(REVISIONS), header=True).withColumn("period", F.col("period").cast("date"))
    rev = rev.withColumn("_run_at", F.col("_run_at").cast("timestamp"))
    for col, typ in DECIMALS.items():
        rev = rev.withColumn(f"{col}_new", F.col(f"{col}_new").cast(typ)).withColumn(f"{col}_old", F.col(f"{col}_old").cast(typ))

    silver = spark.read.csv(str(SILVER), header=True).withColumn("period", F.col("period").cast("date"))
    silver = silver.withColumn("weight_kg", F.col("weight_kg").cast(DECIMALS["weight_kg"])) \
                   .withColumn("value_eur", F.col("value_eur").cast(DECIMALS["value_eur"]))

    out = revisions_summary(rev, products, silver).orderBy("run_at", "period", "flow", "product")
    pdf = out.toPandas()
    # fixed notation for Power BI: no 5.227e-05
    pdf["abs_value_delta_share_of_month"] = pdf["abs_value_delta_share_of_month"].map(
        lambda v: None if v is None or v != v else format(v, ".10f").rstrip("0").rstrip("."))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(OUT, index=False)
    print(f"{len(pdf)} rows -> {OUT}")


if __name__ == "__main__":
    main()
