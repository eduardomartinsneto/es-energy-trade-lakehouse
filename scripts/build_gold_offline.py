"""Rebuild the Gold tables for Power BI from local exports, without Databricks (same functions as the notebooks).

Inputs  (not in git): data/silver_offline/trade_flows_aeat.csv        Silver (legacy export + 2026 rebuilt from the August file)
                      data/samples/Clasi_new_fich.xlsx                AEAT classifications workbook (sheet "Clasif- Países")
Outputs (not in git): <out>/energy_imports_monthly.csv, supply_concentration.csv, dim_country.csv
                      (``revisions_summary.csv`` comes from build_revisions_summary_offline.py)

Numbers are written in fixed notation (no ``1e-05``) with ``.`` as decimal separator.
Run from the repository root:  python scripts/build_gold_offline.py [--out DIR]
"""
from __future__ import annotations

import argparse
import pathlib
import sys

import openpyxl
import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from es_energy_trade.transforms.dim import country_rows  # noqa: E402
from es_energy_trade.transforms.gold import energy_imports_monthly, supply_concentration  # noqa: E402

SILVER = ROOT / "data" / "silver_offline" / "trade_flows_aeat.csv"
WORKBOOK = ROOT / "data" / "samples" / "Clasi_new_fich.xlsx"


def fixed(value: float | None) -> str | None:
    """Float -> plain decimal text, trailing zeros trimmed (``0.0000123``, never ``1.23e-05``)."""
    if value is None or pd.isna(value):
        return None
    return format(value, ".10f").rstrip("0").rstrip(".")


def write(df: pd.DataFrame, path: pathlib.Path, float_cols: tuple[str, ...] = ()) -> None:
    for col in float_cols:
        df[col] = df[col].map(fixed)
    df.to_csv(path, index=False)
    print(f"{len(df):>5} rows -> {path}")


def main(out: pathlib.Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master("local[2]").appName("gold-offline")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    products = yaml.safe_load((ROOT / "config" / "sources.yml").read_text())["products"]

    silver = (spark.read.csv(str(SILVER), header=True)
              .withColumn("period", F.col("period").cast("date"))
              .withColumn("weight_kg", F.col("weight_kg").cast("decimal(18,3)"))
              .withColumn("value_eur", F.col("value_eur").cast("decimal(18,2)")))
    imports = energy_imports_monthly(silver, products).cache()
    sort = ["period", "product"]
    write(imports.orderBy(*sort, "origin_code").toPandas(), out / "energy_imports_monthly.csv", ("share_of_weight",))
    write(supply_concentration(imports).orderBy(*sort).toPandas(), out / "supply_concentration.csv", ("top1_share",))

    wb = openpyxl.load_workbook(WORKBOOK, read_only=True, data_only=True)
    rows = country_rows(wb["Clasif- Países"].iter_rows(values_only=True))
    dim = pd.DataFrame(rows, columns=["country_code", "country_name", "is_country"])
    write(dim, out / "dim_country.csv")
    used = set(imports.select("origin_code").distinct().toPandas()["origin_code"])
    missing = used - set(dim["country_code"])
    assert not missing, f"origin codes without a country row: {sorted(missing)}"
    print(f"all {len(used)} origin codes in use have a dim_country row; "
          f"{len(used & set(dim.loc[~dim['is_country'], 'country_code']))} of them are special codes")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=pathlib.Path, default=ROOT / "data" / "gold_offline")
    main(ap.parse_args().out)
