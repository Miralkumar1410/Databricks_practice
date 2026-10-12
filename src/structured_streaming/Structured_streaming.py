
# STEP 1: Set the path to the first CSV file and validate it as batch data.

source_path = (
    "/Volumes/dev_catalog/structured_straming_lab/"
    "source/events_batch1.csv"
)

csv_batch = (
    spark.read
    .option("header", "true")       # First CSV row contains column names.
    .option("inferSchema", "true")  # Infer types for this validation read only.
    .csv(source_path)
)

print("Total rows in batch CSV:", csv_batch.count())
display(csv_batch)



# STEP 2: Define the explicit schema for streaming CSV ingestion.
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)

schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

print("Explicit streaming schema created.")


# STEP 3: Read CSV files as a streaming DataFrame.

from pyspark.sql import functions as F

source_dir = (
    "/Volumes/dev_catalog/structured_straming_lab/source"
)

csv_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(source_dir)
)

print("Is streaming:", csv_stream.isStreaming)


# STEP 4: Apply an event-time watermark and aggregate by one-minute window.

aggregated_stream = (
    csv_stream
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        F.window("event_time", "1 minute"),
        F.col("event_type")
    )
    .agg(
        F.count("*").alias("event_count")
    )
)

print("Watermarked window aggregation created.")
print("Is aggregation streaming:", aggregated_stream.isStreaming)



# STEP 5: Configure checkpoint and Delta output locations.
checkpoint_path = (
    "/Volumes/dev_catalog/structured_straming_lab/"
    "source/watermark_checkpoint"
)

output_path = (
    "/Volumes/dev_catalog/structured_straming_lab/"
    "source/aggregated_events"
)

print("Checkpoint path:", checkpoint_path)
print("Delta output path:", output_path)


# STEP 6: Start the query and process files currently available.

query = (
    aggregated_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(output_path)
)

query.awaitTermination()

print("Query active after completion:", query.isActive)
print("Query exception:", query.exception())


# STEP 7: Read and inspect the Delta output.

result_df = (
    spark.read
    .format("delta")
    .load(output_path)
)

print("Total aggregate rows:", result_df.count())
display(result_df)



# STEP 8: Inspect the last streaming progress report.
print("Last progress:", query.lastProgress)


# STEP 9: Restart the completed query using the SAME checkpoint and output.
restarted_query = (
    aggregated_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(output_path)
)

restarted_query.awaitTermination()

print("Restart completed:", not restarted_query.isActive)
print("Restart exception:", restarted_query.exception())


# STEP 10: Verify output after restart.
result_after_restart = (
    spark.read
    .format("delta")
    .load(output_path)
)

print("Rows after restart:", result_after_restart.count())
display(result_after_restart)



# STEP 11 Incremental ingestion with a second CSV file.
incremental_query = (
    aggregated_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(output_path)
)

incremental_query.awaitTermination()

print("Incremental run completed:", not incremental_query.isActive)
print("Incremental run exception:", incremental_query.exception())

# STEP 12: Inspect final output after incremental ingestion.
final_result_df = (
    spark.read
    .format("delta")
    .load(output_path)
)

print("Total aggregate rows after incremental run:", final_result_df.count())
display(final_result_df)


