# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · `portfolio.silver.trade_flows_aeat`
# MAGIC Parses Bronze files (legacy ZIPs 2019 → 2025-12, latest 2026 CSV snapshot), aggregates to the Silver grain,
# MAGIC runs the fail-the-run gates, records revisions, then `MERGE`s. Thin notebook: logic is in `src/`.

# COMMAND ----------
import datetime
import os
import pathlib
import sys

sys.path.append(os.path.abspath("../src"))

from es_energy_trade.ingestion.aeat_download import ensure_manifest, latest_csv_snapshot
from es_energy_trade.ingestion.aeat_reader import read_2026, read_legacy
from es_energy_trade.quality.gates import check_period_floor, check_release_coverage, check_silver
from es_energy_trade.transforms.aeat import parse_2026, parse_legacy, to_silver
from es_energy_trade.transforms.revisions import changed_rows
from es_energy_trade.transforms.silver_merge import create_table_sql, merge_sql

dbutils.widgets.text("volume_root", "/Volumes/portfolio/bronze/raw_files")
dbutils.widgets.dropdown("source", "legacy", ["legacy", "csv2026"])
dbutils.widgets.text("snapshot", "latest")  # csv2026 only: "latest" or a snapshot date such as 2026-10-16
dbutils.widgets.text("silver_table", "portfolio.silver.trade_flows_aeat")
dbutils.widgets.text("revisions_table", "portfolio.silver.trade_flows_aeat_revisions")

volume_root = dbutils.widgets.get("volume_root")
source = dbutils.widgets.get("source")
silver_table = dbutils.widgets.get("silver_table")
revisions_table = dbutils.widgets.get("revisions_table")

spark.sql("CREATE SCHEMA IF NOT EXISTS portfolio.silver")

# COMMAND ----------
# Build the batch
if source == "legacy":
    files = sorted(str(p) for p in pathlib.Path(volume_root, "aeat", "legacy").glob("*/cg*39") if p.suffix != ".zip")
    assert files, "No unpacked legacy files in the volume: run notebook 01 first"
    print(f"{len(files)} legacy monthly files")
    batch = to_silver(parse_legacy(read_legacy(spark, files)))
else:
    wanted = dbutils.widgets.get("snapshot")
    snapshot = (latest_csv_snapshot(volume_root) if wanted == "latest" else
                pathlib.Path(volume_root, "aeat", "csv_2026", f"snapshot={wanted}", "MAXIMA_DESAGREGACION_2026.csv"))
    assert snapshot is not None and snapshot.exists(), "No such CSV snapshot in the volume: run notebook 02 first"
    print(f"CSV snapshot: {snapshot}")
    batch = to_silver(parse_2026(read_2026(spark, str(snapshot))))

batch = batch.cache()

# COMMAND ----------
# Gates that fail the run (ADR-008)
print(check_silver(batch))
if source == "csv2026":
    check_period_floor(batch, "2026-01-01")  # ADR-015: legacy ZIPs own 2019-2025
    claimed = ensure_manifest(snapshot)["through_month"]  # None for a snapshot uploaded by hand
    print("release covers up to", check_release_coverage(batch, "2026-01-01", claimed))

# COMMAND ----------
# Revisions must be computed BEFORE the MERGE overwrites the old values
spark.sql(create_table_sql(silver_table))
run_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

revised = changed_rows(batch, spark.table(silver_table))
n_revised = revised.count()
if n_revised:
    from pyspark.sql import functions as F
    (revised.withColumn("_run_at", F.lit(run_at).cast("timestamp"))
        .write.mode("append").option("mergeSchema", "true").saveAsTable(revisions_table))
print(f"{n_revised} revised rows recorded in {revisions_table}")

# COMMAND ----------
batch.createOrReplaceTempView("silver_batch")
result = spark.sql(merge_sql(silver_table, "silver_batch")).collect()[0].asDict()
print(result)  # num_affected_rows / num_updated_rows / num_inserted_rows
