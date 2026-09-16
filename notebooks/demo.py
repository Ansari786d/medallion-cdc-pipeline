# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Retail CDC Pipeline — End-to-End Demo
# MAGIC Runs Bronze ingestion → Silver incremental merge → Gold aggregation, in order.
# MAGIC Use this notebook to demo the pipeline or to manually trigger a run.

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2
# MAGIC
# MAGIC import sys
# MAGIC sys.path.append("../src")
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC from utils import load_config, get_logger
# MAGIC from bronze_ingest import ingest_new_file
# MAGIC from silver_incremental import run_silver_incremental
# MAGIC from gold_aggregate import run_gold_aggregate
# MAGIC
# MAGIC
# MAGIC logger = get_logger("demo")
# MAGIC cfg = load_config("../config/config.yaml")

# COMMAND ----------

# MAGIC %md ### Step 1 — Ingest a new raw batch file into Bronze
# MAGIC Change `batch_file` below to whichever file you're testing:
# MAGIC `batch1_initial_load.csv`, `batch2_new_data.csv`, or `batch3_schema_change.csv`

# COMMAND ----------

batch_file = "corrupt_data_v2.csv"   # <-- change this per run
file_path = cfg["raw_file_path"] + batch_file

rows_ingested = ingest_new_file(spark, file_path, cfg)
logger.info(f"Ingested {rows_ingested} rows from {batch_file}")

# COMMAND ----------

# MAGIC %md ### Step 2 — Run incremental Silver merge

# COMMAND ----------

silver_summary = run_silver_incremental(spark, cfg)
logger.info(f"Silver run summary: {silver_summary}")

# COMMAND ----------

# MAGIC %md ### Step 3 — Refresh Gold summary

# COMMAND ----------

run_gold_aggregate(spark, cfg)

# COMMAND ----------

# MAGIC %md ### Step 4 — Inspect results

# COMMAND ----------

catalog = cfg["catalog"]
display(spark.table(f"{catalog}.silver.transactions_clean").orderBy("updated_at", ascending=False))

# COMMAND ----------

# display(spark.table(f"{catalog}.gold.daily_summary"))

# COMMAND ----------

# MAGIC %md ### Step 5 — Check dead-letter (rejected) rows, if any

# COMMAND ----------

from utils import table_exists

rejected_table = f"{catalog}.silver.transactions_rejected"
if table_exists(spark, rejected_table):
    display(spark.table(rejected_table))
else:
    print("No rejected rows table yet — no bad data encountered so far.")

# COMMAND ----------

