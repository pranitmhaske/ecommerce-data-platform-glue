from pyspark.sql import functions as F
from pyspark.sql import Window


def dedupe(df, dataset_name=None):

    # USERS dataset: canonical key = user_id
    if dataset_name == "users":
        order_col = "updated_at" if "updated_at" in df.columns else "created_at"
        w = Window.partitionBy("user_id").orderBy(F.col(order_col).desc_nulls_last())

        df = (
            df.withColumn("rank", F.row_number().over(w))
              .filter(F.col("rank") == 1)
              .drop("rank")
        )
        return df

    # EVENTS dataset: canonical key = event_id, order by ts
    if dataset_name == "events":
        w = Window.partitionBy("event_id").orderBy(F.col("ts").desc_nulls_last())

        df = (
            df.withColumn("rank", F.row_number().over(w))
              .filter(F.col("rank") == 1)
              .drop("rank")
        )
        return df

    # TRANSACTIONS dataset: canonical key = tx_id
    if dataset_name == "transactions":
        w = Window.partitionBy("tx_id").orderBy(F.col("created_at").desc_nulls_last())

        df = (
            df.withColumn("rank", F.row_number().over(w))
              .filter(F.col("rank") == 1)
              .drop("rank")
        )
        return df

    return df
