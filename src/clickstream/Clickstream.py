# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)
from pyspark.sql import functions as F

# Source folder: new CSV files will be added here one batch at a time.
source_dir = (
    "/Volumes/dev_catalog/structured_straming_lab/"
    "source/clickstream_source"
)

# Define the CSV structure explicitly.
clickstream_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("page", StringType(), True),
    StructField("action", StringType(), True)
])

# Read files incrementally as a streaming DataFrame.
clickstream_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(clickstream_schema)
    .load(source_dir)
)

print("Is streaming:", clickstream_stream.isStreaming)

# COMMAND ----------

# A two-minute watermark controls late-event handling.
# A five-minute session gap groups events from the same user.
sessionized_stream = (
    clickstream_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        F.col("user_id"),
        F.session_window(
            F.col("event_time"),
            "5 minutes"
        )
    )
    .agg(
        F.count("*").alias("event_count"),
        F.collect_set("action").alias("actions"),
        F.collect_set("page").alias("pages")
    )
)

print("Sessionized streaming DataFrame created.")

# COMMAND ----------

checkpoint_path = (
    "/Volumes/dev_catalog/structured_straming_lab/"
    "source/clickstream_checkpoint"
)

silver_path = (
    "/Volumes/dev_catalog/structured_straming_lab/"
    "source/silver_sessions"
)

print("Checkpoint:", checkpoint_path)
print("Silver output:", silver_path)

# COMMAND ----------

def process_clickstream_batch():
    query = (
        sessionized_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(silver_path)
    )

    query.awaitTermination()

    print("Query completed:", not query.isActive)
    print("Query exception:", query.exception())

    # Inspect the latest progress report, including watermark metrics.
    progress = query.lastProgress

    if progress:
        print("Input rows:", progress.get("numInputRows"))
        print("Event-time information:", progress.get("eventTime"))
        print("State operator metrics:", progress.get("stateOperators"))

    return query

# COMMAND ----------

query1 = process_clickstream_batch()

# COMMAND ----------

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

display(silver_df)

# COMMAND ----------

query2 = process_clickstream_batch()

# COMMAND ----------

query3 = process_clickstream_batch()

# COMMAND ----------

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

display(silver_df.orderBy("user_id"))

# COMMAND ----------

query4 = process_clickstream_batch()

# COMMAND ----------

progress = query4.lastProgress

if progress:
    print("Event-time metrics:")
    print(progress.get("eventTime"))

    print("State operator metrics:")
    print(progress.get("stateOperators"))

# COMMAND ----------

final_silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

print("Final Silver session count:", final_silver_df.count())

display(final_silver_df.orderBy("user_id"))