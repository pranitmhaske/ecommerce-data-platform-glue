from pyspark.sql import functions as F
from pyspark.sql import types as T


NULL_STRINGS = {"", " ", "  ", "NaN", "nan", "NULL", "null", "None", "NONE", "N/A", "n/a"}

# cleans null like strings and convert them to real null
def normalize_nulls(df):
    for col, dtype in df.dtypes:
        df = df.withColumn(
            col,
            F.when(F.trim(F.col(col).cast("string")).isin(NULL_STRINGS), None)
             .otherwise(F.col(col))
        )
    return df

# this function cleans whitespaces from string datatypes
def trim_strings(df):
    for col, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col, F.trim(F.col(col)))
    return df

# this function removes commas and empty strings
def sanitize_numeric(df, col):
    if col not in df.columns:
        return df
    return df.withColumn(
        col,
        F.when(F.col(col).isNull(), None)
         .otherwise(F.regexp_replace(F.col(col).cast("string"), ",", ""))
    )

# parse timestamp parse for wrong formats
def parse_timestamp(df, col):
    if col not in df.columns:
        return df

    raw = f"{col}__raw"
    df = df.withColumn(raw, F.col(col).cast("string"))

    df = df.withColumn(col, F.to_timestamp(raw))

    df = df.withColumn(
        col,
        F.coalesce(
            F.col(col),
            F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(raw, "yyyy-MM-dd")
        )
    )

    return df.drop(raw)

# cleans datasets using previous functions
def clean_events(df):
    df = normalize_nulls(df)
    df = trim_strings(df)

    # Strong timestamp parser
    df = parse_timestamp(df, "ts")

    # Numeric cleaning
    for col in ("price", "amount", "temperature", "age"):
        df = sanitize_numeric(df, col)

    return df

def clean_users(df):
    df = normalize_nulls(df)
    df = trim_strings(df)

    # timestamp
    df = parse_timestamp(df, "updated_at")

    # numeric
    df = sanitize_numeric(df, "age")

    return df

def clean_transactions(df):
    df = normalize_nulls(df)
    df = trim_strings(df)

    # timestamps
    df = parse_timestamp(df, "created_at")
    df = parse_timestamp(df, "delivered_at")

    # numeric
    df = sanitize_numeric(df, "amount")
    df = sanitize_numeric(df, "age")

    return df

# main
def clean_columns(df, dataset_name):
    d = dataset_name.lower()

    if d == "events":
        return clean_events(df)

    if d == "users":
        return clean_users(df)

    if d == "transactions":
        return clean_transactions(df)

    return df
