"""
purpose: Land raw data exactly as it arrived. No cleaning, no filtering, no deduplication.
Here we will keep copy of our raw data as it is so that when anything goes wrong then we can reproduce our source data 
instead of reading from source again.

Bronze Layer

Purpose: Land raw data exactly as it arrived. No cleaning, no filtering, no deduplication.

What happens here:

Read the incoming file (CSV) using an explicit schema (not inferred)
Append it to the Bronze table — never overwrite, never delete
Track metadata about where it came from (source file path, ingestion timestamp) — this supports idempotency later
Even a completely broken/null/garbage row still lands here, untouched

Why it exists: so you always have a faithful, replayable copy of "what did the source actually send." If Silver logic has a bug, you reprocess from Bronze — you never have to go back to the original source system.
"""

from schemas import BRONZE_SCHEMA
from pyspark.sql.functions import col, current_timestamp
from utils import get_logger, load_config, table_exists


logger = get_logger("Bronze_Layer")
config = load_config()


def ingest_new_file(spark, file_path, config):
    catalog = config["catalog"]
    bronze_schema = config["schemas"]["bronze"]
    bronze_table = config["tables"]["bronze_raw"]
    full_table_name = f"{catalog}.{bronze_schema}.{bronze_table}"

    logger.info(f"Reading raw file: {file_path}")

    raw_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(BRONZE_SCHEMA)
        .load(file_path)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modified_at", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", current_timestamp())
    )
    raw_df.show(truncate=False)

    # Idempotency guard: skip files we've already ingested (by filename),
    # so re-running the job after a failure doesn't duplicate Bronze data.
    if table_exists(spark, full_table_name):
        already_ingested_rows = (
            spark.table(full_table_name)
            .select("_source_file", "_file_modified_at")
            .distinct()
            .collect()
        )
        already_ingested = [(row["_source_file"], row["_file_modified_at"]) for row in already_ingested_rows]

        current_file_info = (raw_df.select("_source_file", "_file_modified_at").distinct().collect()[0])
        current_key = (current_file_info[0], current_file_info[1])
        if current_key in already_ingested:
            logger.info(f"Skipping already ingested file: {file_path}")
            return 0
        
    count = raw_df.count()
    logger.info(f"Appending {count} rows from raw file: {file_path} to {full_table_name}")
    (
        raw_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(full_table_name)
    )

    logger.info(f"Finished writing raw file to table: {full_table_name}")
    return count



if __name__ == "__main__":
    cfg = load_config()
    # In production this path would come from a job parameter/widget,
    # not hardcoded — shown explicitly here for standalone-run clarity.
    input_file = cfg["raw_file_path"] + "batch1_initial_load.csv"
    ingest_new_file(spark, input_file, cfg)