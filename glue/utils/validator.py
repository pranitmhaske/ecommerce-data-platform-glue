from pyspark.sql import functions as F
from utils.rules import dq_check_not_null, dq_check_unique, dq_check_timestamp

# -----------------------------------------------------------
# Compute total rows + per-column null stats (optimized)
# -----------------------------------------------------------
def compute_null_stats(df):
    total = df.count()
    if total == 0:
        return total, {c: 0 for c in df.columns}

    # Single aggregation instead of scanning per-column
    exprs = [
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]

    result = df.agg(*exprs).collect()[0].asDict()
    stats = {col: round(result[col] / total, 4) for col in df.columns}

    return total, stats


# -----------------------------------------------------------
# Write metrics DataFrame to CSV
# -----------------------------------------------------------
def write_metrics(spark, dataset_name, base_path, metrics):
    rows = [(k, str(v)) for k, v in metrics.items()]
    df = spark.createDataFrame(rows, ["metric", "value"])
    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{base_path}/{dataset_name}")
    )


# -----------------------------------------------------------
# Validator: compute metrics & return df unchanged
# -----------------------------------------------------------
def validate(df, spark, dataset_name, metrics_path):
    metrics = {}

    # ----------------------------------------
    # 1. Total row count + null stats
    # ----------------------------------------
    total_rows, null_stats = compute_null_stats(df)
    metrics["total_rows"] = total_rows

    for col_name, rate in null_stats.items():
        metrics[f"null_rate_{col_name}"] = rate

    # ----------------------------------------
    # 2. RULE CHECKS
    # ----------------------------------------

    # not-null always
    for col in df.columns:
        ok, bad = dq_check_not_null(df, col)
        metrics[f"dq_not_null_{col}"] = bad

    # unique rule only for users
    if dataset_name == "users" and "user_id" in df.columns:
        ok, bad = dq_check_unique(df, "user_id")
        metrics["dq_unique_user_id"] = bad

    # timestamp validity checks
    for tcol in ["ts", "event_timestamp", "updated_at", "created_at"]:
        if tcol in df.columns:
            ok, bad = dq_check_timestamp(df, tcol)
            metrics[f"dq_timestamp_{tcol}"] = bad

    # ----------------------------------------
    # 3. Write metrics
    # ----------------------------------------
    write_metrics(spark, dataset_name, metrics_path, metrics)
    return df
