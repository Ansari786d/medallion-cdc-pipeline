"""
schemas.py

Explicit schema definitions for each layer.

Why this matters: inferSchema=True is convenient for exploration but risky in
production — it silently guesses types from a data sample, which can change
between runs (e.g. a column that's all-integers in batch 1 but has one decimal
value in batch 2 flips the inferred type). Defining schemas explicitly makes
the pipeline predictable and is what real production pipelines do.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Schema for the raw Bronze landing table.
# Kept intentionally permissive (most fields as StringType) since Bronze's job
# is just to capture raw data faithfully — type casting/cleaning happens in Silver.
BRONZE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("product", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),      # cast to Double in Silver
    StructField("updated_at", StringType(), nullable=True),  # cast to Timestamp in Silver
    StructField("payment_method", StringType(), nullable=True),  # added later (schema evolution)
])

# Schema for the cleaned Silver table — this is the "contract" downstream
# consumers (Gold, BI tools) rely on. Strict types, nullable=False on required fields.
SILVER_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("product", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("payment_method", StringType(), nullable=True),
])
