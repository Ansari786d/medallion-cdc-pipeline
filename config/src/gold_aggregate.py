"""
gold_aggregate.py

Purpose: produce one simple, business-ready summary table from Silver.

Design decision: full overwrite, not incremental.
Gold aggregates are cheap to recompute from Silver (Silver is already small
and clean), so there's no need for incremental complexity here — that
complexity belongs in Silver, where the expensive raw-data processing happens.
Keeping Gold simple is itself a deliberate choice, not an oversight.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, sum as spark_sum, count as spark_count

from utils import get_logger, load_config


logger = get_logger("gold_aggregate")


def run_gold_aggregate(spark: SparkSession, config: dict) -> None:
    catalog = config["catalog"]
    silver_table = f"{catalog}.{config['schemas']['silver']}.{config['tables']['silver_clean']}"
    gold_table = f"{catalog}.{config['schemas']['gold']}.{config['tables']['gold_summary']}"

    logger.info(f"Reading from {silver_table}")
    silver_df = spark.table(silver_table)

    daily_summary = (
        silver_df
        .groupBy(to_date("updated_at").alias("date"))
        .agg(
            spark_sum("amount").alias("total_amount"),
            spark_count("transaction_id").alias("transaction_count"),
        )
        .orderBy("date")
    )

    logger.info(f"Writing daily summary to {gold_table}")
    (
        daily_summary.write.format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )
    logger.info("Gold aggregation complete.")


if __name__ == "__main__":
    # spark = SparkSession.builder.getOrCreate()
    cfg = load_config()
    run_gold_aggregate(spark, cfg)
