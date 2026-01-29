from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T


def dict_to_structtype(schema_dict):
    field_type_map = {
        "string": T.StringType(),
        "integer": T.IntegerType(),
        "double": T.DoubleType(),
        "timestamp": T.TimestampType(),
    }
    fields = [
        T.StructField(col, field_type_map.get(dtype, T.StringType()), True)
        for col, dtype in schema_dict.items()
    ]
    return T.StructType(fields)


# -----------------------------
# Canonical schemas 
# -----------------------------
EVENTS_SCHEMA = {
    "event_id": "string",
    "user_id": "string",
    "ts": "timestamp",            
    "age": "integer",
    "country": "string",
    "email": "string",
    "message": "string",
    "name": "string",
    "price": "double",
    "temperature": "double",
    "amount": "double",
    "year": "integer",
    "month": "integer",
    "day": "integer"
}

USERS_SCHEMA = {
    "user_id": "string",
    "name": "string",
    "email": "string",
    "phone": "string",
    "city": "string",
    "age": "integer",
    "updated_at": "timestamp",
    "year": "integer",
    "month": "integer",
    "day": "integer"
}

TRANSACTIONS_SCHEMA = {
    "tx_id": "string",
    "user_id": "string",
    "amount": "double",
    "status": "string",
    "created_at": "timestamp",
    "delivered_at": "timestamp",
    "email": "string",
    "name": "string",
    "age": "integer",
    "year": "integer",
    "month": "integer",
    "day": "integer"
}

SCHEMAS = {
    "events": EVENTS_SCHEMA,
    "users": USERS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA
}

# -----------------------------
# Utilities
# -----------------------------

def _is_extra_column(col):
    """Return True if column looks like extra_* junk (but NOT _corrupt_record)."""
    return col.startswith("extra_")


def _sanitize_numeric_col_expr(col_expr):
    """Return expression that safely converts common numeric-as-string issues to numeric.
    Removes commas and whitespace, handles empty strings.
    """
    return F.when(
        (F.col(col_expr).isNull()) | (F.trim(F.col(col_expr)) == ""),
        None
    ).otherwise(
        F.regexp_replace(F.col(col_expr), ",", "")
    )


def _safe_cast(df, col, target):
    """Cast a column safely to target type.
    target: 'integer', 'double', 'timestamp', 'string'
    Returns df with column casted.
    """
    if target == "string":
        return df.withColumn(col, F.col(col).cast("string"))

    if target in ("integer", "double"):
        # remove non-numeric characters commonly found in dirty datasets
        cleaned = _sanitize_numeric_col_expr(col)  # Column expression
        if target == "integer":
            return df.withColumn(col, cleaned.cast("long"))
        else:
            return df.withColumn(col, cleaned.cast("double"))

    if target == "timestamp":
        # Try multiple common timestamp formats. If parsing fails, keep null.
        # First try ISO format, then a few common alternatives.
        return df.withColumn(
            col,
            F.coalesce(
                F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(F.col(col), "yyyy-MM-dd")
            )
        )

    # fallback - try generic cast
    return df.withColumn(col, F.col(col).cast(target))


# -----------------------------
# Main normalizer
# -----------------------------

def normalize_schema(df, dataset_name, allow_quarantine=True, drop_unknown=True):
    """Normalize `df` according to canonical schema for `dataset_name`.

    Returns:
      (clean_df, quarantine_df) if allow_quarantine True
      clean_df if allow_quarantine False

    Behavior:
      - Drops columns named extra_*
      - Keeps only canonical fields + partition columns
      - Adds missing columns as nulls
      - Casts columns to canonical types safely
      - Any row failing basic required-field checks will be returned in quarantine_df
        when allow_quarantine=True
    """
    dataset_name = dataset_name.lower()
    if dataset_name not in SCHEMAS:
        raise ValueError(f"Unknown dataset_name: {dataset_name}")

    canonical = SCHEMAS[dataset_name]
    canonical_cols = list(canonical.keys())
    canonical_struct = dict_to_structtype(canonical)

    # 1) Drop obvious junk columns first to avoid huge wide dataframes
    cols_to_drop = [c for c in df.columns if _is_extra_column(c)]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    # 2) If drop_unknown=True, drop any column not in canonical or partition cols
    if drop_unknown:
        keep_set = set(canonical_cols)
        # allow partition columns even if not in canonical (year/month/day)
        keep_set.update({"year", "month", "day"})
        to_drop = [c for c in df.columns if c not in keep_set]
        if to_drop:
            df = df.drop(*to_drop)

    # 3) Ensure all canonical columns exist (add nulls for missing)
    for c in canonical_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))

    # 4) Quarantine rows with _corrupt_record if present
    quarantine_df = None
    if "_corrupt_record" in df.columns:
        quarantine_df = df.filter(F.col("_corrupt_record").isNotNull())
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # 5) Basic required-field validation per dataset (so pipeline doesn't blow up)
    required_checks = []
    if dataset_name == "events":
        required_checks = [F.col("event_id").isNotNull(), F.col("user_id").isNotNull()]
    elif dataset_name == "users":
        required_checks = [F.col("user_id").isNotNull()]
    elif dataset_name == "transactions":
        required_checks = [F.col("tx_id").isNotNull(), F.col("user_id").isNotNull()]

    if required_checks:
        condition = reduce(lambda a, b: a & b, required_checks)
        failed = df.filter(~condition)
        if allow_quarantine:
            if quarantine_df is None:
                quarantine_df = failed
            else:
                quarantine_df = quarantine_df.unionByName(failed, allowMissingColumns=True)
        df = df.filter(condition)

    # 6) Cast types safely for canonical columns
    for col, dtype in canonical.items():
        if dtype == "string":
            df = _safe_cast(df, col, "string")
        elif dtype == "integer":
            df = _safe_cast(df, col, "integer")
        elif dtype == "double":
            df = _safe_cast(df, col, "double")
        elif dtype == "timestamp":
            df = _safe_cast(df, col, "timestamp")
        else:
            df = _safe_cast(df, col, dtype)

    # 7) Normalize column naming: strip whitespace (but keep canonical names)
    for c in df.columns:
        clean_name = c.strip()
        if clean_name != c:
            df = df.withColumnRenamed(c, clean_name)

    # 8) Ensure partition columns are integers
    for p in ("year", "month", "day"):
        if p in df.columns:
            df = _safe_cast(df, p, "integer")

    # 9) Final ordering (canonical columns first, then partition columns)
    final_cols = [c for c in canonical_cols if c in df.columns]
    for p in ("year", "month", "day"):
        if p in df.columns and p not in final_cols:
            final_cols.append(p)

    df = df.select(*final_cols)

    if allow_quarantine:
        # ensure quarantine_df has same schema (add missing cols)
        if quarantine_df is None:
            # return empty DF with same schema
            empty = df.limit(0)
            return df, empty

        # add missing cols in quarantine
        for c in df.columns:
            if c not in quarantine_df.columns:
                quarantine_df = quarantine_df.withColumn(c, F.lit(None))
        quarantine_df = quarantine_df.select(df.columns)
        return df, quarantine_df

    return df
