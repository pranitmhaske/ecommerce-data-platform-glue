from pyspark.sql import functions as F

#------------------------------------------------
# USER SCD MERGE
#------------------------------------------------

def merge_user_history(df):

    # canonical primary key
    pk = "user_id"

    # canonical timestamp column
    order_col = "updated_at" if "updated_at" in df.columns else "created_at"

    # fallback timestamp to avoid NULL ordering
    df = df.withColumn(
        "ts_fixed",
        F.coalesce(F.col(order_col), F.lit("1900-01-01").cast("timestamp"))
    )

    # SCD Type-1 merge (latest per user_id)
    filled = df.groupBy(pk).agg(
        F.max_by("name", "ts_fixed").alias("name"),
        F.max_by("email", "ts_fixed").alias("email"),
        F.max_by("phone", "ts_fixed").alias("phone"),
        F.max_by("city", "ts_fixed").alias("city"),
        F.max_by("age", "ts_fixed").alias("age"),
        F.max("ts_fixed").alias("updated_at")
    )

    return filled
