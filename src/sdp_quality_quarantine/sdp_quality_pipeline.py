
from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp


# 1. BRONZE: Read raw CSV records
@dp.table(name="orders_bronze_raw")
def orders_bronze_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(
            "/Volumes/dev_catalog/default/sdp_lab/lab2/"
        )
    )


# 2. SILVER: Keep valid records
@dp.table(name="orders_silver_valid")
@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL"
)
@dp.expect_or_drop(
    "positive_amount",
    "amount IS NOT NULL AND amount > 0"
)
@dp.expect_or_drop(
    "valid_timestamp",
    "order_ts IS NOT NULL"
)
def orders_silver_valid():

    df = (
        spark.readStream.table("orders_bronze_raw")
        .withColumn(
            "order_id",
            col("order_id").cast("int")
        )
        .withColumn(
            "amount",
            col("amount").cast("double")
        )
        .withColumn(
            "order_ts",
            to_timestamp(
                col("order_ts"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    return (
        df.filter(col("order_id").isNotNull())
        .filter(col("amount") > 0)
        .filter(col("order_ts").isNotNull())
    )


# 3. QUARANTINE: Keep invalid records
@dp.table(name="orders_quarantine")
def orders_quarantine():

    df = (
        spark.readStream.table("orders_bronze_raw")
        .withColumn(
            "order_id_num",
            col("order_id").cast("int")
        )
        .withColumn(
            "amount_num",
            col("amount").cast("double")
        )
        .withColumn(
            "order_ts_parsed",
            to_timestamp(
                col("order_ts"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    return df.filter(
        col("order_id_num").isNull()
        | col("amount_num").isNull()
        | (col("amount_num") <= 0)
        | col("order_ts_parsed").isNull()
    )
