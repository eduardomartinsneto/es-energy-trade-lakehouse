# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze · AEAT legacy ZIPs (2019 → Dec 2025)
# MAGIC Discovers the chapter 23–39 ZIP of every month, downloads it into the volume and unpacks it.
# MAGIC Thin notebook: all logic lives in `src/es_energy_trade/ingestion/`. Re-runnable: finished months are skipped.

# COMMAND ----------
# MAGIC %pip install beautifulsoup4 requests

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))  # notebook sits in notebooks/, package in src/

from es_energy_trade.ingestion.aeat_discovery import discover_year, energy_files, missing_months
from es_energy_trade.ingestion.aeat_download import download_and_unpack, http_fetch

dbutils.widgets.text("volume_root", "/Volumes/portfolio/bronze/raw_files")
dbutils.widgets.text("from_period", "2019-01")
dbutils.widgets.text("to_period", "2025-12")  # 2026 months come from the cumulative CSV (source of truth)

volume_root = dbutils.widgets.get("volume_root")
start = tuple(int(x) for x in dbutils.widgets.get("from_period").split("-"))
end = tuple(int(x) for x in dbutils.widgets.get("to_period").split("-"))

# COMMAND ----------
# Discovery: reads the site, builds nothing from patterns.
files = []
for year in range(start[0], end[0] + 1):
    files += energy_files(discover_year(year, http_fetch))
files = [f for f in files if start <= (f.year, f.month) <= end]

gaps = missing_months(files, start, end)
if gaps:
    raise RuntimeError(f"No chapter 23-39 file found for: {gaps}")
print(f"{len(files)} monthly files to load, {start} -> {end}")

# COMMAND ----------
failures = []
for f in files:
    try:
        path = download_and_unpack(f, volume_root)
        print(f"ok   {f.year}-{f.month:02d}  {path}  {path.stat().st_size / 1e6:,.0f} MB")
    except Exception as exc:  # keep going: one bad month must not hide the others
        failures.append((f.year, f.month, repr(exc)))
        print(f"FAIL {f.year}-{f.month:02d}  {exc!r}")

if failures:
    raise RuntimeError(f"{len(failures)} month(s) failed: {failures}")
