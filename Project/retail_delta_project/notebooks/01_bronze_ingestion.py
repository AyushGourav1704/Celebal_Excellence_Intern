# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Raw Ingestion
# MAGIC Reads everything as strings (no casting, no strict schema), tags every row
# MAGIC with `source_file`, `ingestion_ts`, `load_type`, and lands it as Delta.
# MAGIC
# MAGIC - Historical **batch** files -> one-time `spark.read` -> `overwrite` Bronze tables.
# MAGIC - Daily **incremental/CDC** files -> Auto Loader (`cloudFiles`) so you never have
# MAGIC   to track "which files have I already processed" yourself.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Historical batch loads

# COMMAND ----------

def read_batch_csv(path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # keep everything as string in Bronze
        .csv(path)
    )


def add_audit_cols(df, load_type):
    return (
        df
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("load_type", F.lit(load_type))
    )


batch_tables = {
    "bronze_orders": f"{BATCH_PATH}/orders_batch.csv",
    "bronze_customers": f"{BATCH_PATH}/customers_batch.csv",
    "bronze_products": f"{BATCH_PATH}/products_batch.csv",
    "bronze_stores": f"{BATCH_PATH}/stores_batch.csv",
}

for table_name, path in batch_tables.items():
    df = add_audit_cols(read_batch_csv(path), "batch")
    full_name = f"{CATALOG_PREFIX}{RAW_SCHEMA}.{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_name))
    print(f"loaded {full_name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Incremental / CDC loads via Auto Loader
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) tracks which files it has already seen for you —
# MAGIC that's the "process only new files without tracking state manually" hint from
# MAGIC the guide. Each stream gets its own checkpoint + schema location.
# MAGIC
# MAGIC **Schema evolution note:** `orders_incremental_2026-04-26.csv` introduces a new
# MAGIC `coupon_code` column. With `cloudFiles.schemaEvolutionMode = "addNewColumns"`,
# MAGIC Auto Loader will deliberately **fail the micro-batch the first time it sees the
# MAGIC new column** and update its schema log — this is the `UnknownFieldException` /
# MAGIC "Detected schema change" error from the guide's common-errors table. Just
# MAGIC re-run the cell; the second run picks up the new schema and continues. That's
# MAGIC expected behavior, not a bug.

def autoload_stream(source_glob, table_name, checkpoint_subdir):
    full_name = f"{CATALOG_PREFIX}{RAW_SCHEMA}.{table_name}"
    checkpoint = f"{CHECKPOINT_PATH}/{checkpoint_subdir}"
    schema_loc = f"{SCHEMA_PATH}/{checkpoint_subdir}"

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", False)   # keep Bronze as strings
        .load(source_glob)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("load_type", F.lit("incremental"))
    )

    query = (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)     # process what's there and stop (batch-style run)
        .toTable(full_name)
    )
    query.awaitTermination()
    print(f"streamed into {full_name}")


autoload_stream(f"{INCR_PATH}/*/orders_incremental_*.csv",
                 "bronze_orders_incremental", "orders_incremental")

autoload_stream(f"{INCR_PATH}/*/customers_cdc_*.csv",
                 "bronze_customers_cdc", "customers_cdc")

autoload_stream(f"{INCR_PATH}/*/products_cdc_*.csv",
                 "bronze_products_cdc", "products_cdc")

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

for t in ["bronze_orders", "bronze_orders_incremental", "bronze_customers",
          "bronze_customers_cdc", "bronze_products", "bronze_products_cdc",
          "bronze_stores"]:
    full_name = f"{CATALOG_PREFIX}{RAW_SCHEMA}.{t}"
    n = spark.table(full_name).count()
    print(f"{full_name}: {n} rows")
