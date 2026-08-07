# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Stage 1: Clean / Cast / Deduplicate / Quarantine
# MAGIC Converts raw strings into typed columns, drops rows that fail basic validity
# MAGIC checks into `*_quarantine` tables, and deduplicates using
# MAGIC "latest ingestion wins" per business key.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ## Orders

# COMMAND ----------

orders_raw = (
    spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_orders")
    .unionByName(
        spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_orders_incremental")
        .drop("coupon_code"),
        allowMissingColumns=True,
    )
)

orders_typed = (
    orders_raw
    # strip currency symbols / letters out of unit_price, keep digits, '.', '-'
    .withColumn("unit_price_clean", F.regexp_extract(F.col("unit_price"), r"(-?\d+\.?\d*)", 1))
    .withColumn("unit_price_cast", F.col("unit_price_clean").cast("double"))
    .withColumn("quantity_cast", F.col("quantity").cast("int"))
    .withColumn("order_date_cast", F.to_date("order_date", "yyyy-MM-dd"))
)

is_valid_order = (
    F.col("order_id").isNotNull() & (F.col("order_id") != "") &
    F.col("customer_id").isNotNull() & (F.col("customer_id") != "") &
    F.col("product_id").isNotNull() & (F.col("product_id") != "") &
    F.col("order_date_cast").isNotNull() &
    F.col("unit_price_cast").isNotNull() & (F.col("unit_price_cast") > 0) &
    F.col("quantity_cast").isNotNull() & (F.col("quantity_cast") > 0)
)

orders_good = orders_typed.filter(is_valid_order)
orders_quarantine = orders_typed.filter(~is_valid_order)

# dedup: newest ingestion wins per order_id
w_orders = Window.partitionBy("order_id").orderBy(F.col("ingestion_ts").desc())
orders_clean = (
    orders_good
    .withColumn("rn", F.row_number().over(w_orders))
    .filter("rn = 1")
    .drop("rn", "unit_price_clean")
    .select("order_id", "customer_id", "product_id", "store_id",
            F.col("order_date_cast").alias("order_date"),
            F.col("quantity_cast").alias("quantity"),
            F.col("unit_price_cast").alias("unit_price"),
            "coupon_code" if "coupon_code" in orders_typed.columns else F.lit(None).alias("coupon_code"),
            "source_file", "ingestion_ts", "load_type")
)

(orders_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_orders_clean"))
(orders_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_orders_quarantine"))

print("orders_clean:", orders_clean.count(), "| orders_quarantine:", orders_quarantine.count())

# COMMAND ----------

# MAGIC %md ## Customers

# COMMAND ----------

customers_raw = spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_customers")

customers_typed = (
    customers_raw
    .withColumn("signup_date_cast", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("city_clean", F.when((F.col("city").isNull()) | (F.col("city") == ""), "Unknown")
                               .otherwise(F.col("city")))
    .withColumn("segment_clean", F.when((F.col("segment").isNull()) | (F.col("segment") == ""), "Unknown")
                                  .otherwise(F.col("segment")))
)

is_valid_customer = F.col("customer_id").isNotNull() & (F.col("customer_id") != "")

customers_good = customers_typed.filter(is_valid_customer)
customers_quarantine = customers_typed.filter(~is_valid_customer)

w_cust = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())
customers_clean = (
    customers_good
    .withColumn("rn", F.row_number().over(w_cust))
    .filter("rn = 1")
    .select("customer_id", "customer_name", "email", "city_clean",
            "segment_clean", F.col("signup_date_cast").alias("signup_date"),
            "source_file", "ingestion_ts", "load_type")
    .withColumnRenamed("city_clean", "city")
    .withColumnRenamed("segment_clean", "segment")
)

(customers_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_customers_clean"))
(customers_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_customers_quarantine"))

print("customers_clean:", customers_clean.count(), "| customers_quarantine:", customers_quarantine.count())

# COMMAND ----------

# MAGIC %md ## Products

# COMMAND ----------

products_raw = spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_products")

products_typed = (
    products_raw
    .withColumn("unit_price_clean", F.regexp_extract(F.col("unit_price"), r"(\d+\.?\d*)", 1))
    .withColumn("unit_price_cast", F.col("unit_price_clean").cast("double"))
    .withColumn("category_clean", F.when((F.col("category").isNull()) | (F.col("category") == ""), "Unknown")
                                   .otherwise(F.col("category")))
)

is_valid_product = (
    F.col("product_id").isNotNull() & (F.col("product_id") != "") &
    F.col("unit_price_cast").isNotNull() & (F.col("unit_price_cast") > 0)
)

products_good = products_typed.filter(is_valid_product)
products_quarantine = products_typed.filter(~is_valid_product)

w_prod = Window.partitionBy("product_id").orderBy(F.col("ingestion_ts").desc())
products_clean = (
    products_good
    .withColumn("rn", F.row_number().over(w_prod))
    .filter("rn = 1")
    .select("product_id", "product_name", "category_clean",
            F.col("unit_price_cast").alias("unit_price"),
            "source_file", "ingestion_ts", "load_type")
    .withColumnRenamed("category_clean", "category")
)

(products_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_products_clean"))
(products_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_products_quarantine"))

print("products_clean:", products_clean.count(), "| products_quarantine:", products_quarantine.count())

# COMMAND ----------

# MAGIC %md ## Stores
# MAGIC No CDC feed for stores in this project — treat as a simple (non-SCD) dimension.

# COMMAND ----------

stores_raw = spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_stores")

w_store = Window.partitionBy("store_id").orderBy(F.col("ingestion_ts").desc())
dim_store = (
    stores_raw
    .withColumn("rn", F.row_number().over(w_store))
    .filter("rn = 1")
    .select("store_id", "store_name", "city", "region")
)

(dim_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.dim_store"))

print("dim_store:", dim_store.count())
