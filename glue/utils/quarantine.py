import logging
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from utils.logger import get_logger

# -------------------------------------------------------------------
# Logger setup
# -------------------------------------------------------------------
logger = get_logger(__name__)

# -------------------------------------------------------------------
# Safe column accessor
# -------------------------------------------------------------------
def _safe_col(df: DataFrame, col_name: str):
    """
    Return a column if it exists, otherwise a literal NULL
    """
    if df is None or col_name not in df.columns:
        return F.lit(None)
    return F.col(col_name)


# -------------------------------------------------------------------
# Dataset specific quarantine conditions
# -------------------------------------------------------------------
def get_quarantine_conditions(df: DataFrame, dataset_name: str):
    """
    Returns a boolean Spark expression marking rows that should be quarantined
    """
    default_false = F.lit(False)

    if df is None:
        return default_false

    # -------------------- EVENTS --------------------
    if dataset_name == "events":
        return (
            _safe_col(df, "event_id").isNull() |
            _safe_col(df, "user_id").isNull() |
            _safe_col(df, "ts").isNull() |
            _safe_col(df, "_corrupt_record").isNotNull()
        )

    # -------------------- USERS --------------------
    if dataset_name == "users":
        return (
            _safe_col(df, "user_id").isNull() |
            _safe_col(df, "_corrupt_record").isNotNull()
        )

    # -------------------- TRANSACTIONS --------------------
    if dataset_name == "transactions":
        created_missing = _safe_col(df, "created_at").isNull()
        txid_missing = _safe_col(df, "tx_id").isNull()
        corrupt = _safe_col(df, "_corrupt_record").isNotNull()

        amount_col = _safe_col(df, "amount")
        amount_negative = F.when(
            amount_col.isNull(), F.lit(False)
        ).otherwise(amount_col < 0)

        return txid_missing | created_missing | corrupt | amount_negative

    # Default → allow all
    return default_false


# -------------------------------------------------------------------
# Sanitize void/unknown columns
# -------------------------------------------------------------------
def sanitize_void_columns(df: DataFrame):
    """
    Replace any void/null/unknown typed Spark columns with a known safe type
    """
    if df is None:
        return df

    for name, dtype in df.dtypes:
        if str(dtype).lower() in ("void", "null", "unknown"):
            df = df.withColumn(name, F.lit(None).cast("string"))

    return df


# -------------------------------------------------------------------
# Main quarantine function
# -------------------------------------------------------------------
def quarantine_rows(df: DataFrame, dataset_name: str, output_base_path: str):
    """
    Split dataset into good + bad rows using quarantine rules.
    BAD records are written to:

        s3://bucket/quarantine/<dataset_name>/

    Returns:
        good_df, bad_df
    """

    if df is None:
        logger.warning("quarantine_rows called with df=None")
        return None, None

    # Determine quarantine condition safely
    try:
        condition = get_quarantine_conditions(df, dataset_name)
        if condition is None:
            logger.warning("Condition None, defaulting to no quarantine.")
            condition = F.lit(False)
    except Exception:
        logger.exception("Error building quarantine condition — defaulting to no quarantine")
        condition = F.lit(False)

    # Split datasets
    bad = df.filter(condition)
    good = df.filter(~condition)

    bad_count = bad.count()
    good_count = good.count()

    logger.info(f"[QUARANTINE] {dataset_name}: good={good_count}, bad={bad_count}")

    # Write bad rows to S3 path: base/dataset_name
    if bad_count > 0:
        bad = sanitize_void_columns(bad)
        target = f"{output_base_path}/{dataset_name}"

        logger.info(f"[QUARANTINE] Writing {bad_count} rows → {target}")

        (
            bad.write
            .mode("append")
            .option("compression", "snappy")
            .parquet(target)
        )
    else:
        logger.info("[QUARANTINE] No bad rows to write")
