"""
Silver Layer — the core, worth the most thinking time

Purpose: Turn raw Bronze data into clean, trustworthy, deduplicated data — incrementally, not by reprocessing everything each time.

The functions/steps, in the order they must happen:

Find the watermark — what's the latest timestamp already processed in Silver? (None, on the very first run.)
Type-cast tolerantly — convert string columns to real types (amount → double, updated_at → timestamp), using "tolerant" casting so a bad value becomes null instead of crashing the whole job.
Separate out rows with no usable timestamp — before filtering by watermark. This is the subtle one you already learned the hard way: a null timestamp compared against the watermark silently disappears if you don't pull it out first.
Apply the watermark filter — now that timestamps are safe, keep only rows newer than the last watermark. This is what makes it incremental, not a full reprocess.
Validate required fields — split rows into valid vs. rejected based on whether required columns (transaction_id, amount, updated_at) are actually filled in.
Deduplicate the valid rows — if the same key appears more than once in this batch, keep only the most recent version. (This is what prevents the MERGE ambiguous-match error.)
Write rejected rows to a dead-letter table — tagged with why they were rejected, so bad data is visible, not silently dropped.
MERGE the valid, deduped rows into Silver — upsert logic: update if the key already exists, insert if it's new. Handle the very first run specially, since MERGE needs a target table to already exist.
"""
from pyspark.sql.functions import col, lit, to_timestamp, row_number, max as spark_max, coalesce, expr
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession 
from utils import get_logger, load_config, table_exists
from delta.tables import DeltaTable



logger = get_logger('silver layer')

def get_watermark(spark, table_name, water_mark_col):
    if not table_exists(spark, table_name):
        return None 
    result = spark.table(table_name).agg(spark_max(water_mark_col)).collect()[0][0]
    return result 


def split_by_timestamp_validity(df, watermark_col):
    """
    Splits rows into (rows with a usable timestamp, rows without one).
    MUST run before the watermark filter — a null timestamp compared against
    the watermark evaluates to null (not false), so it silently disappears
    from the result if this isn't done first.
    """
    has_valid_timestamp = col(watermark_col).isNotNull()

    timestamp_missing_df = (
        df.filter(~has_valid_timestamp)
        .withColumn("_rejection_reason", lit("unparseable_or_missing_timestamp"))
    )
    timestamped_df = df.filter(has_valid_timestamp)

    return timestamped_df, timestamp_missing_df




def split_valid_and_rejected(df):
    """
    Splits rows into valid vs rejected based on whether required columns are filled in.
    """
    is_bad_row = col("transaction_id").isNull() | col("amount").isNull() | col("updated_at").isNull()
    valid_df = df.filter(~is_bad_row)
    rejected_df = df.filter(is_bad_row).withColumn("_rejection_reason", lit("missing_required_field"))
    return valid_df, rejected_df

def deduplicate(df, key_col, order_col):
    """
    Deduplicates the valid rows based on the key columns.
    """
    window = Window.partitionBy(col(key_col)).orderBy(col(order_col).desc())
    return (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

def run_silver_incremental(spark, config):
    """
    Main entry point: reads new Bronze rows since the last watermark,
    cleans, dedupes, splits valid/rejected, and merges into Silver.
    Returns a summary dict for logging/monitoring.
    """
    catalog = config["catalog"]
    bronze_table = f"{catalog}.{config['schemas']['bronze']}.{config['tables']['bronze_raw']}"
    silver_table = f"{catalog}.{config['schemas']['silver']}.{config['tables']['silver_clean']}"
    rejected_table = f"{catalog}.{config['schemas']['silver']}.{config['tables']['silver_rejected']}"
    dedup_key = config["dedup_key"]
    dedup_order = config["dedup_order_by"]
    

    watermark = get_watermark(spark, silver_table, dedup_order)
    logger.info(f"Current Watermark is {watermark}")

    bronze_df = spark.table(bronze_table)

    #Type castig in silver table 
    bronze_df = (
        bronze_df
        .withColumn("amount", expr("try_cast(amount AS double)"))
        .withColumn(
            "updated_at",
            coalesce(
                expr("try_to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss')"),
                expr("try_to_timestamp(updated_at, 'dd-MM-yyyy HH:mm')")
            )
        )
    )
    total_rows = bronze_df.count()
    logger.info(f"Total rows in Bronze: {total_rows}")

    # STEP 1: split off rows with no usable timestamp BEFORE the watermark filter
    timestamped_df, timestamp_missing_df = split_by_timestamp_validity(bronze_df, dedup_order)
    timestamp_missing_count = timestamp_missing_df.count()
    if timestamp_missing_count > 0:
        logger.warning(f"{timestamp_missing_count} rows have no valid timestamp — routing to dead-letter")

    # STEP 2: apply watermark filter only to rows with a valid timestamp
    new_rows = (
        timestamped_df.filter(col(dedup_order) > watermark) if watermark is not None else timestamped_df
    )
    
    new_count = new_rows.count()
    logger.info(f"New rows since last watermark: {new_count}")

    if new_count == 0 and timestamp_missing_count == 0:
        logger.info("No new data to process. Exiting.")
        return {"new_rows": 0, "valid_rows": 0, "rejected_rows": 0}
    
    # STEP 3: validate required fields
    valid_df, rejected_df = split_valid_and_rejected(new_rows)
    valid_df = deduplicate(valid_df, dedup_key, dedup_order)
    
    valid_count = valid_df.count()
    logger.info(f"After de duplication the total valid rows: {valid_count}")
    rejected_count = rejected_df.count()
    logger.info(f"Rows that has missing required fields: {rejected_count}")

    # STEP 4: combine both rejection reasons into one dead-letter write
    all_rejected_df = rejected_df.unionByName(timestamp_missing_df, allowMissingColumns=True)
    total_rejected_count = rejected_count + timestamp_missing_count

    logger.info(f"Valid rows: {valid_count} | Rejected rows (missing_required_fields, unparseable_or_missing_timestamp): {total_rejected_count}")

    # --- Write rejected rows to dead-letter table (append, for audit trail) ---
    if total_rejected_count > 0:
        (
            all_rejected_df.write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(rejected_table)
        )
        logger.warning(f"{total_rejected_count} rows written to dead-letter table: {rejected_table}")

    ## STEP 5: merge valid rows into Silver
    if valid_count > 0:
        if not table_exists(spark, silver_table):
            logger.info(f"Silver table doesn't exist yet — creating: {silver_table}")
            valid_df.write.format("delta").saveAsTable(silver_table)
        else:
            target_table = DeltaTable.forName(spark, silver_table)
            (
                target_table.alias("t").merge(
                    source=valid_df.alias("s"),
                    condition=f"t.{dedup_key} == s.{dedup_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"{valid_count} rows merged into Silver: {silver_table}")
    else:
        logger.info("No valid rows to merge this run.")

    return {
        "new_rows": new_count,
        "valid_rows": valid_count,
        "rejected_rows": total_rejected_count,
    }

if __name__ == "__main__":
    cfg = load_config()
    summary = run_silver_incremental(spark, cfg)
    logger.info(f"Run summary: {summary}")
















 

