"""
utils.py

Shared, reusable helpers used across all pipeline stages.
Keeping these separate from the pipeline logic keeps bronze/silver/gold
scripts focused only on their own transformation logic.
"""

import logging
import yaml
from pyspark.sql import DataFrame


def get_logger(name: str) -> logging.Logger:
    """
    Standard structured logger, used instead of print() statements.
    In production, these logs flow into job run logs / monitoring tools,
    so consistent formatting matters for debugging failed runs later.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def load_config(config_path: str = "../config/config.yaml") -> dict:
    """
    Load pipeline configuration from YAML.
    Never hardcode table names/paths directly in pipeline scripts —
    always read from here so environments (dev/prod) can differ by config alone.
    """
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def table_exists(spark, full_table_name: str) -> bool:
    """Check if a Unity Catalog table exists, without raising an exception."""
    try:
        return spark.catalog.tableExists(full_table_name)
    except Exception:
        return False


def row_count(df: DataFrame) -> int:
    """Small wrapper so logging a row count reads cleanly in pipeline code."""
    return df.count()
