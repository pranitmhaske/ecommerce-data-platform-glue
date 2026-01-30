from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _empty_df(spark: SparkSession):
    """Return valid empty DataFrame with empty schema."""
    return spark.createDataFrame(
        spark.sparkContext.emptyRDD(),
        T.StructType([])
    )


# -------------------------------------------------------------------
# Distributed-safe limited failure readers
# -------------------------------------------------------------------
def _read_parquet_if_exists(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.parquet(f"{path}/*")
    except Exception:
        return _empty_df(spark)


def _read_json_if_exists(spark: SparkSession, path: str) -> DataFrame:
    try:
        return (
            spark.read
                .option("multiLine", False)
                .option("mode", "PERMISSIVE")
                .option("badRecordsPath", f"{path}/_quarantine")
                .json(f"{path}/*.json")
        )
    except Exception:
        try:
            return (
                spark.read
                    .option("multiLine", False)
                    .option("mode", "PERMISSIVE")
                    .option("badRecordsPath", f"{path}/_quarantine")
                    .json(f"{path}/*.ndjson")
            )
        except Exception:
            return _empty_df(spark)


def _read_csv_if_exists(spark: SparkSession, path: str) -> DataFrame:
    try:
        return (
            spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(f"{path}/*.csv")
        )
    except Exception:
        return _empty_df(spark)


def _read_gz_if_exists(spark: SparkSession, path: str) -> DataFrame:
    try:
        if any(x in path.lower() for x in ["json", "ndjson"]):
            return (
                spark.read
                    .option("multiLine", False)
                    .option("mode", "PERMISSIVE")
                    .option("badRecordsPath", f"{path}/_quarantine")
                    .json(f"{path}/*.gz")
            )

        return (
            spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(f"{path}/*.gz")
        )

    except Exception:
        return _empty_df(spark)


def _read_txt_if_exists(spark: SparkSession, path: str) -> DataFrame:
    try:
        if "json" in path.lower():
            return (
                spark.read
                    .option("multiLine", False)
                    .option("mode", "PERMISSIVE")
                    .option("badRecordsPath", f"{path}/_quarantine")
                    .json(f"{path}/*.txt")
            )

        return (
            spark.read
                .option("inferSchema", True)
                .option("header", False)
                .csv(f"{path}/*.txt")
        )

    except Exception:
        return _empty_df(spark)


# -------------------------------------------------------------------
# Merge spark DataFrames preserving schema
# -------------------------------------------------------------------
def union_preserve_schema(dfs):
    all_cols = []
    for df in dfs:
        for c in df.columns:
            if c not in all_cols:
                all_cols.append(c)

    if not all_cols:
        spark = dfs[0].sparkSession
        return spark.createDataFrame(
            spark.sparkContext.emptyRDD(),
            T.StructType([])
        )

    normalized = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        for m in missing:
            df = df.withColumn(m, F.lit(None))
        normalized.append(df.select(*all_cols))

    base = normalized[0]
    for d in normalized[1:]:
        base = base.unionByName(d)

    return base


# -------------------------------------------------------------------
# PRODUCTION LOADER — BRONZE
# -------------------------------------------------------------------
from utils.schema_normalizer import SCHEMAS, dict_to_structtype


def load_bronze_data(spark: SparkSession, bronze_path: str):
    dataset_name = bronze_path.split("/")[-1]

    schema_dict = SCHEMAS.get(dataset_name, None)
    schema_struct = dict_to_structtype(schema_dict) if schema_dict else None

    parquet_df = _read_parquet_if_exists(spark, bronze_path)
    json_df    = _read_json_if_exists(spark, bronze_path)
    csv_df     = _read_csv_if_exists(spark, bronze_path)
    gz_df      = _read_gz_if_exists(spark, bronze_path)
    txt_df     = _read_txt_if_exists(spark, bronze_path)

    def normalize_columns(df: DataFrame) -> DataFrame:
        return df.toDF(*[
            c.replace("`", "")
             .replace(".", "_")
             .replace(" ", "_")
             .strip()
            for c in df.columns
        ])

    parquet_df = normalize_columns(parquet_df)
    json_df    = normalize_columns(json_df)
    csv_df     = normalize_columns(csv_df)
    gz_df      = normalize_columns(gz_df)
    txt_df     = normalize_columns(txt_df)

    df = union_preserve_schema([
        parquet_df,
        json_df,
        csv_df,
        gz_df,
        txt_df,
    ])

    if schema_struct:

        def cast_to_schema(df: DataFrame, schema_struct: T.StructType) -> DataFrame:
            exprs = []
            for f in schema_struct.fields:
                if f.name in df.columns:
                    exprs.append(F.col(f.name).cast(f.dataType).alias(f.name))
                else:
                    exprs.append(F.lit(None).cast(f.dataType).alias(f.name))
            return df.select(*exprs)

        actual_cols = df.columns
        expected_cols = [f.name for f in schema_struct.fields]

        missing = [c for c in expected_cols if c not in actual_cols]
        extra = [c for c in actual_cols if c not in expected_cols]

        print(f"[BRONZE] missing={missing}, extra={len(extra)}")

        df = cast_to_schema(df, schema_struct)

    return df


# -------------------------------------------------------------------
# PRODUCTION LOADER — SILVER
# -------------------------------------------------------------------
def load_silver_data(spark: SparkSession, silver_path: str):
    parquet_df = _read_parquet_if_exists(spark, silver_path)
    json_df    = _read_json_if_exists(spark, silver_path)
    csv_df     = _read_csv_if_exists(spark, silver_path)

    df = union_preserve_schema([parquet_df, json_df, csv_df])

    if "id" in df.columns and "user_id" not in df.columns:
        df = df.withColumn("user_id", F.col("id"))

    if "id" in df.columns:
        df = df.drop("id")

    path_lower = silver_path.lower()
    if "transaction" in path_lower and "tx_id" not in df.columns:
        df = df.withColumn("tx_id", F.col("user_id"))

    return df
