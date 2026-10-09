# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_catalog.autoloader_lab;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dev_catalog.autoloader_lab.raw_data;

# COMMAND ----------

display(
    spark.read.csv(
        "/Volumes/dev_catalog/autoloader_lab/raw_data/orders_01.csv",
        header=True,
        inferSchema=True
    )
)


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev_catalog.autoloader_lab.orders_bronze;

# COMMAND ----------

source_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/orders/"
)

schema_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/"
    "schemas/orders/"
)

checkpoint_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/"
    "checkpoints/orders_bronze/"
)

# COMMAND ----------

orders_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_path
        )
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )
        .load(source_path)
)

# COMMAND ----------

query = (
    orders_stream.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(
            "dev_catalog.autoloader_lab.orders_bronze"
        )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev_catalog.autoloader_lab.orders_bronze
# MAGIC ORDER BY customer_id;

# COMMAND ----------

source_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/orders/"
)

schema_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/"
    "schemas/orders/"
)

checkpoint_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/"
    "checkpoints/orders_bronze/"
)

orders_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_path
        )
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )
        .load(source_path)
)

# COMMAND ----------

query = (
    orders_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(
            "dev_catalog.autoloader_lab.orders_bronze"
        )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev_catalog.autoloader_lab.orders_bronze
# MAGIC ORDER BY customer_id;

# COMMAND ----------

source_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/orders/"
)

schema_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/"
    "schemas/orders/"
)

checkpoint_path = (
    "/Volumes/dev_catalog/autoloader_lab/raw_data/"
    "checkpoints/orders_bronze/"
)

orders_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_path
        )
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )
        .load(source_path)
)

query = (
    orders_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(
            "dev_catalog.autoloader_lab.orders_bronze"
        )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev_catalog.autoloader_lab.orders_bronze
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     product,
# MAGIC     amount,
# MAGIC     discount_code,
# MAGIC     _rescued_data
# MAGIC FROM dev_catalog.autoloader_lab.orders_bronze
# MAGIC WHERE customer_id = 7;