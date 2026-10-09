
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# 1. Read customer change events
@dp.table(name="customer_changes")
def customer_changes():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/dev_catalog/default/sdp_lab/lab3/")
        .withColumn(
            "sequence_num",
            col("sequence_num").cast("int")
        )
    )


# 2. Create the SCD Type 2 target
dp.create_streaming_table(
    name="dim_customer_scd2"
)


# 3. Apply inserts, updates, and deletes
dp.create_auto_cdc_flow(
    target="dim_customer_scd2",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2
)
