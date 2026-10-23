# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze · AEAT 2026 cumulative CSV ("nuevo formato") — source of truth for 2026
# MAGIC The file is republished with all months (and revisions). Each real change is kept as a dated snapshot.

# COMMAND ----------
# MAGIC %pip install beautifulsoup4 requests

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import datetime
import os
import sys

sys.path.append(os.path.abspath("../src"))

from es_energy_trade.ingestion.aeat_discovery import CSV_2026_PAGE, find_csv_2026
from es_energy_trade.ingestion.aeat_download import download_csv_snapshot, http_fetch

dbutils.widgets.text("volume_root", "/Volumes/portfolio/bronze/raw_files")
volume_root = dbutils.widgets.get("volume_root")

# COMMAND ----------
link = find_csv_2026(http_fetch(CSV_2026_PAGE), CSV_2026_PAGE)
print(f"CSV covers January to month {link.through_month}: {link.url}")

path, downloaded = download_csv_snapshot(link.url, volume_root, today=datetime.date.today().isoformat())
print(("downloaded" if downloaded else "unchanged since last snapshot"), path, f"{path.stat().st_size / 1e6:,.0f} MB")
