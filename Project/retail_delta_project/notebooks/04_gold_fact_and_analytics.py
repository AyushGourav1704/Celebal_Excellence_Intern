# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Layer: Conformed Fact Table + Analytics Tables
# MAGIC Resolves each order to the dimension row that was active *at the time of the
# MAGIC transaction* (point-in-time SCD2 join), then builds the dashboard-ready
# MAGIC aggregate tables.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = spark.table(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_orders_clean")
dim_customer = spark.table(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.dim_customer_scd2")
dim_product = spark.table(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.dim_product_scd2")
dim_store = spark.table(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.dim_store")

# COMMAND ----------

# MAGIC %md ## Point-in-time join to resolve surrogate keys
# MAGIC `order_date` must fall between `effective_start_date` and `effective_end_date`
# MAGIC of the dimension row — a plain equi-join on the business key alone would give
# MAGIC you whichever version happens to match, not the one that was true when the
# MAGIC order was placed.

# COMMAND ----------

fact_orders_pit = (
    orders.alias("o")
    .join(
        dim_customer.alias("c"),
        (F.col("o.customer_id") == F.col("c.customer_id")) &
        (F.col("o.order_date") >= F.col("c.effective_start_date")) &
        (F.col("o.order_date") <= F.col("c.effective_end_date")),
        "left",
    )
    .join(
        dim_product.alias("p"),
        (F.col("o.product_id") == F.col("p.product_id")) &
        (F.col("o.order_date") >= F.col("p.effective_start_date")) &
        (F.col("o.order_date") <= F.col("p.effective_end_date")),
        "left",
    )
    .join(dim_store.alias("s"), F.col("o.store_id") == F.col("s.store_id"), "left")
    .select(
        "o.order_id",
        F.col("c.customer_id_sk").alias("customer_sk"),
        F.col("p.product_id_sk").alias("product_sk"),
        F.col("s.store_id").alias("store_sk"),
        "o.order_date",
        "o.quantity",
        "o.unit_price",
        (F.col("o.quantity") * F.col("o.unit_price")).alias("revenue"),
        "o.coupon_code",
        F.col("p.category").alias("product_category"),
        F.col("c.segment").alias("customer_segment"),
        F.col("s.region").alias("store_region"),
        "o.customer_id",
        "o.product_id",
    )
)

# COMMAND ----------

# MAGIC %md ## Fallback for orders that predate their dimension's earliest version
# MAGIC A handful of orders can have a date earlier than the customer/product's first
# MAGIC known dimension record (e.g. a late-registered signup date). Rather than
# MAGIC leaving the surrogate key null for that case, fall back to the **earliest
# MAGIC available version** so the fact table stays resolved, and keep the fallback
# MAGIC visible via `customer_key_is_estimated` / `product_key_is_estimated`.
# MAGIC
# MAGIC This is different from — and does not fix — **orphaned foreign keys**: orders
# MAGIC whose `product_id`/`customer_id` has *no* dimension row at all (current or
# MAGIC historical), because the master record itself failed Silver Stage 1
# MAGIC validation and no CDC update ever corrected it. Those genuinely have nothing
# MAGIC to fall back to, so they get routed to a referential-integrity quarantine
# MAGIC table below instead of being silently dropped or faked.

# COMMAND ----------

w_cust_first = F.row_number().over(
    Window.partitionBy("customer_id").orderBy("effective_start_date"))
dim_customer_first = (
    dim_customer.withColumn("rn", w_cust_first).filter("rn = 1")
    .select(F.col("customer_id"), F.col("customer_id_sk").alias("customer_sk_fallback"),
            F.col("segment").alias("customer_segment_fallback"))
)

w_prod_first = F.row_number().over(
    Window.partitionBy("product_id").orderBy("effective_start_date"))
dim_product_first = (
    dim_product.withColumn("rn", w_prod_first).filter("rn = 1")
    .select(F.col("product_id"), F.col("product_id_sk").alias("product_sk_fallback"),
            F.col("category").alias("product_category_fallback"))
)

fact_orders = (
    fact_orders_pit
    .join(dim_customer_first, on="customer_id", how="left")
    .join(dim_product_first, on="product_id", how="left")
    .withColumn("customer_key_is_estimated", F.col("customer_sk").isNull() & F.col("customer_sk_fallback").isNotNull())
    .withColumn("product_key_is_estimated", F.col("product_sk").isNull() & F.col("product_sk_fallback").isNotNull())
    .withColumn("customer_sk", F.coalesce("customer_sk", "customer_sk_fallback"))
    .withColumn("product_sk", F.coalesce("product_sk", "product_sk_fallback"))
    .withColumn("customer_segment", F.coalesce("customer_segment", "customer_segment_fallback"))
    .withColumn("product_category", F.coalesce("product_category", "product_category_fallback"))
    .drop("customer_sk_fallback", "product_sk_fallback", "customer_segment_fallback", "product_category_fallback")
)

# MAGIC %md ## Split out referential-integrity failures
# MAGIC Orders whose product/customer never resolved to *any* dimension row (not
# MAGIC even via the fallback) go to a quarantine table instead of `fact_orders`,
# MAGIC so Gold aggregates aren't silently skewed by orphaned rows and you have an
# MAGIC auditable list of exactly which orders/products need a data-quality fix
# MAGIC upstream (e.g. a product whose price was never corrected by a later CDC
# MAGIC update).

# COMMAND ----------

is_resolved = F.col("customer_sk").isNotNull() & F.col("product_sk").isNotNull()
fact_orders_resolved = fact_orders.filter(is_resolved)
fact_orders_referential_quarantine = fact_orders.filter(~is_resolved)

(fact_orders_resolved.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.fact_orders"))
(fact_orders_referential_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.fact_orders_referential_quarantine"))

estimated = fact_orders_resolved.filter("customer_key_is_estimated or product_key_is_estimated").count()
print("fact_orders rows:", fact_orders_resolved.count(),
      "| resolved via earliest-version fallback:", estimated,
      "| routed to referential-integrity quarantine (orphaned FK, no dim row exists):",
      fact_orders_referential_quarantine.count())

# COMMAND ----------

# MAGIC %md Which product/customer IDs are actually causing the orphaned rows —
# MAGIC worth including in your write-up as a found-and-handled data-quality issue.

# COMMAND ----------

display(
    fact_orders_referential_quarantine
    .filter("product_sk is null")
    .join(orders.select("order_id", "product_id"), "order_id")
    .select("product_id").distinct()
)

# COMMAND ----------

# MAGIC %md ## Gold analytics tables

# COMMAND ----------

fact = spark.table(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.fact_orders")

daily_sales = (
    fact.groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.round(F.sum("revenue") / F.countDistinct("order_id"), 2).alias("avg_order_value"),
    )
    .orderBy("order_date")
)
(daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.gold_daily_sales"))

category_sales = (
    fact.groupBy("product_category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_revenue"))
)
(category_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.gold_category_sales"))

segment_sales = (
    fact.groupBy("customer_segment")
    .agg(
        F.countDistinct("customer_sk").alias("unique_customers"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.desc("total_revenue"))
)
(segment_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.gold_segment_sales"))

region_sales = (
    fact.groupBy("store_region")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_revenue"))
)
(region_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG_PREFIX}{GOLD_SCHEMA}.gold_region_sales"))

print("Gold tables built: gold_daily_sales, gold_category_sales, gold_segment_sales, gold_region_sales")
display(daily_sales.limit(10))
