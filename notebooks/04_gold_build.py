# Databricks notebook source
# MAGIC %md
# MAGIC # Gold · `energy_imports_monthly` and `supply_concentration`
# MAGIC Full rebuild from Silver (the Gold tables are small). Business logic lives in `src/es_energy_trade/transforms/gold.py`.

# COMMAND ----------
# MAGIC %pip install pyyaml

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os
import sys

import yaml

sys.path.append(os.path.abspath("../src"))

from es_energy_trade.transforms.gold import energy_imports_monthly, supply_concentration

dbutils.widgets.text("silver_table", "portfolio.silver.trade_flows_aeat")
dbutils.widgets.text("gold_schema", "portfolio.gold")

with open("../config/sources.yml") as fh:
    products = yaml.safe_load(fh)["products"]
# config lists CN prefixes (e.g. crude_oil: ["2709"]); Gold matches them against Silver cn8
print(products)

# COMMAND ----------
gold_schema = dbutils.widgets.get("gold_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

imports = energy_imports_monthly(spark.table(dbutils.widgets.get("silver_table")), products).cache()
imports.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_schema}.energy_imports_monthly")
supply_concentration(imports).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_schema}.supply_concentration")

display(spark.table(f"{gold_schema}.supply_concentration").orderBy("period", "product"))
