from pyspark import pipelines as dp
from pyspark.sql.functions import col


# =========================
# BRONZE
# =========================

@dp.table(
    name="orders_bronze"
)
def orders_bronze():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/dev_catalog/default/sdp_lab/")
    )


# =========================
# SILVER
# =========================

@dp.table(
    name="orders_silver"
)
def orders_silver():

    return (
        spark.readStream
        .table("orders_bronze")
        .select(
            "order_id",
            "customer_name",
            "city",
            "amount",
            "order_ts"
        )
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
            col("order_ts").cast("timestamp")
        )
        .filter(
            col("amount") > 0
        )
    )