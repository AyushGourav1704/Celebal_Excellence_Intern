"""
Local, Databricks-free version of the pipeline, using open-source PySpark +
delta-spark. Runs entirely on your machine from VS Code's terminal.

Differences from the Databricks notebooks (by necessity — these features
don't exist outside Databricks):
  - No dbutils, no Unity Catalog, no Volumes -> plain local folders + a local
    Spark warehouse (Hive metastore in ./local_warehouse).
  - No Auto Loader (`cloudFiles` is Databricks-proprietary) -> incremental
    files are read with a plain batch glob read instead of a streaming
    source. You still get the same Bronze/Silver/Gold tables and the same
    schema-evolution behavior (via unionByName(allowMissingColumns=True)).
  - Everything else — cleaning, SCD2 two-step MERGE, point-in-time Gold join,
    the referential-integrity quarantine — is the same logic as notebooks
    01-04, just without Databricks-only syntax.

Prereqs (see README "Running locally in VS Code" section):
  - Java 11 or 17 on PATH
  - pip install pyspark==3.5.1 delta-spark==3.1.0
"""
import glob
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, Window
from delta.tables import DeltaTable

BASE = os.path.dirname(os.path.abspath(__file__))
BATCH_PATH = os.path.join(BASE, "datasets", "batch")
INCR_PATH = os.path.join(BASE, "datasets", "incremental")
WAREHOUSE_DIR = os.path.join(BASE, "local_warehouse")

RAW_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA = "retail_demo_raw", "retail_demo_silver", "retail_demo_gold"
FAR_FUTURE = "9999-12-31"
INITIAL_LOAD_DATE = "2020-01-01"

# --------------------------------------------------------------------- spark
builder = (
    SparkSession.builder.appName("RetailDeltaLocal")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .config("spark.driver.memory", "4g")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {RAW_SCHEMA}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_SCHEMA}")


def tbl(schema, name):
    return f"{schema}.{name}"


# =========================================================== 01 - BRONZE ==
def add_audit_cols(df, load_type):
    return (df.withColumn("source_file", F.input_file_name())
              .withColumn("ingestion_ts", F.current_timestamp())
              .withColumn("load_type", F.lit(load_type)))


def read_csv_glob(pattern):
    files = sorted(glob.glob(pattern))
    dfs = [spark.read.option("header", True).csv(f) for f in files]
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out


print("=== 01 - Bronze ===")
for name, path in [("bronze_orders", f"{BATCH_PATH}/orders_batch.csv"),
                    ("bronze_customers", f"{BATCH_PATH}/customers_batch.csv"),
                    ("bronze_products", f"{BATCH_PATH}/products_batch.csv"),
                    ("bronze_stores", f"{BATCH_PATH}/stores_batch.csv")]:
    df = add_audit_cols(spark.read.option("header", True).csv(path), "batch")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(RAW_SCHEMA, name))
    print(f"  {name}: {df.count()} rows")

orders_incr = add_audit_cols(read_csv_glob(f"{INCR_PATH}/day_*/orders_incremental_*.csv"), "incremental")
orders_incr.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(RAW_SCHEMA, "bronze_orders_incremental"))
print(f"  bronze_orders_incremental: {orders_incr.count()} rows "
      f"(coupon_code non-null: {orders_incr.filter('coupon_code is not null').count()})")

customers_cdc = add_audit_cols(read_csv_glob(f"{INCR_PATH}/day_*/customers_cdc_*.csv"), "incremental")
customers_cdc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(RAW_SCHEMA, "bronze_customers_cdc"))
print(f"  bronze_customers_cdc: {customers_cdc.count()} rows")

products_cdc = add_audit_cols(read_csv_glob(f"{INCR_PATH}/day_*/products_cdc_*.csv"), "incremental")
products_cdc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(RAW_SCHEMA, "bronze_products_cdc"))
print(f"  bronze_products_cdc: {products_cdc.count()} rows")

# ===================================================== 02 - SILVER STAGE 1
print("\n=== 02 - Silver Stage 1 ===")

orders_raw = (spark.table(tbl(RAW_SCHEMA, "bronze_orders"))
              .unionByName(spark.table(tbl(RAW_SCHEMA, "bronze_orders_incremental")), allowMissingColumns=True))
orders_typed = (
    orders_raw
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
w_orders = Window.partitionBy("order_id").orderBy(F.col("ingestion_ts").desc())
has_coupon = "coupon_code" in orders_typed.columns
orders_clean = (
    orders_good.withColumn("rn", F.row_number().over(w_orders)).filter("rn = 1")
    .select("order_id", "customer_id", "product_id", "store_id",
            F.col("order_date_cast").alias("order_date"),
            F.col("quantity_cast").alias("quantity"),
            F.col("unit_price_cast").alias("unit_price"),
            *(["coupon_code"] if has_coupon else []))
)
orders_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "silver1_orders_clean"))
orders_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "silver1_orders_quarantine"))
print(f"  orders_clean={orders_clean.count()} orders_quarantine={orders_quarantine.count()}")

customers_typed = (
    spark.table(tbl(RAW_SCHEMA, "bronze_customers"))
    .withColumn("signup_date_cast", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("city", F.when((F.col("city").isNull()) | (F.col("city") == ""), "Unknown").otherwise(F.col("city")))
    .withColumn("segment", F.when((F.col("segment").isNull()) | (F.col("segment") == ""), "Unknown").otherwise(F.col("segment")))
)
is_valid_customer = F.col("customer_id").isNotNull() & (F.col("customer_id") != "")
w_cust = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())
customers_clean = (
    customers_typed.filter(is_valid_customer).withColumn("rn", F.row_number().over(w_cust)).filter("rn = 1")
    .select("customer_id", "customer_name", "email", "city", "segment",
            F.col("signup_date_cast").alias("signup_date"))
)
customers_quarantine = customers_typed.filter(~is_valid_customer)
customers_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "silver1_customers_clean"))
customers_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "silver1_customers_quarantine"))
print(f"  customers_clean={customers_clean.count()} customers_quarantine={customers_quarantine.count()}")

products_typed = (
    spark.table(tbl(RAW_SCHEMA, "bronze_products"))
    .withColumn("unit_price_clean", F.regexp_extract(F.col("unit_price"), r"(\d+\.?\d*)", 1))
    .withColumn("unit_price_cast", F.col("unit_price_clean").cast("double"))
    .withColumn("category", F.when((F.col("category").isNull()) | (F.col("category") == ""), "Unknown").otherwise(F.col("category")))
)
is_valid_product = F.col("product_id").isNotNull() & (F.col("product_id") != "") & F.col("unit_price_cast").isNotNull() & (F.col("unit_price_cast") > 0)
w_prod = Window.partitionBy("product_id").orderBy(F.col("ingestion_ts").desc())
products_clean = (
    products_typed.filter(is_valid_product).withColumn("rn", F.row_number().over(w_prod)).filter("rn = 1")
    .select("product_id", "product_name", "category", F.col("unit_price_cast").alias("unit_price"))
)
products_quarantine = products_typed.filter(~is_valid_product)
products_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "silver1_products_clean"))
products_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "silver1_products_quarantine"))
print(f"  products_clean={products_clean.count()} products_quarantine={products_quarantine.count()}")

w_store = Window.partitionBy("store_id").orderBy(F.col("ingestion_ts").desc())
dim_store = (spark.table(tbl(RAW_SCHEMA, "bronze_stores"))
             .withColumn("rn", F.row_number().over(w_store)).filter("rn = 1")
             .select("store_id", "store_name", "city", "region"))
dim_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(SILVER_SCHEMA, "dim_store"))
print(f"  dim_store={dim_store.count()}")

# ===================================================== 03 - SILVER STAGE 2
print("\n=== 03 - Silver Stage 2 (SCD2) ===")


def build_hash(df, attr_cols):
    return df.withColumn("hash_value", F.sha2(F.concat_ws(
        "||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in attr_cols]), 256))


def seed_scd2(source_df, business_key, attr_cols, start_date_col, target):
    seeded = (
        build_hash(source_df, attr_cols)
        .withColumn("effective_start_date", F.coalesce(F.col(start_date_col), F.lit(INITIAL_LOAD_DATE)).cast("date"))
        .withColumn("effective_end_date", F.lit(FAR_FUTURE).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(f"{business_key}_sk", F.sha2(F.concat_ws("||", F.col(business_key), F.col("effective_start_date").cast("string")), 256))
        .select(f"{business_key}_sk", business_key, *attr_cols, "effective_start_date", "effective_end_date", "is_current", "hash_value")
    )
    seeded.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)


def apply_cdc_day(target, cdc_day_df, business_key, attr_cols, change_date_col):
    src = build_hash(cdc_day_df, attr_cols).withColumn(change_date_col, F.col(change_date_col).cast("date"))
    w = Window.partitionBy(business_key).orderBy(F.monotonically_increasing_id().desc())
    src = src.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    dt = DeltaTable.forName(spark, target)
    (dt.alias("t").merge(src.alias("s"), f"t.{business_key} = s.{business_key} AND t.is_current = true")
       .whenMatchedUpdate(condition="t.hash_value <> s.hash_value",
                           set={"is_current": "false", "effective_end_date": f"date_sub(s.{change_date_col}, 1)"})
       .execute())

    current = dt.toDF().filter("is_current = true").select(business_key, "hash_value")
    to_insert = src.join(current, on=[business_key, "hash_value"], how="left_anti")
    if to_insert.take(1):
        new_rows = (
            to_insert.withColumnRenamed(change_date_col, "effective_start_date")
            .withColumn("effective_end_date", F.lit(FAR_FUTURE).cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn(f"{business_key}_sk", F.sha2(F.concat_ws("||", F.col(business_key), F.col("effective_start_date").cast("string")), 256))
            .select(f"{business_key}_sk", business_key, *attr_cols, "effective_start_date", "effective_end_date", "is_current", "hash_value")
        )
        new_rows.write.format("delta").mode("append").saveAsTable(target)
    return to_insert.count()


CUSTOMER_ATTRS = ["customer_name", "email", "city", "segment"]
customer_target = tbl(SILVER_SCHEMA, "dim_customer_scd2")
seed_scd2(spark.table(tbl(SILVER_SCHEMA, "silver1_customers_clean")), "customer_id", CUSTOMER_ATTRS, "signup_date", customer_target)
cust_cdc = spark.table(tbl(RAW_SCHEMA, "bronze_customers_cdc"))
for row in cust_cdc.select("change_date").distinct().orderBy("change_date").collect():
    day = row[0]
    n = apply_cdc_day(customer_target, cust_cdc.filter(F.col("change_date") == day), "customer_id", CUSTOMER_ATTRS, "change_date")
    print(f"  customer CDC {day}: {n} new/changed rows")

PRODUCT_ATTRS = ["product_name", "category", "unit_price"]
product_target = tbl(SILVER_SCHEMA, "dim_product_scd2")
products_seed = spark.table(tbl(SILVER_SCHEMA, "silver1_products_clean")).withColumn("seed_date", F.lit(INITIAL_LOAD_DATE))
seed_scd2(products_seed, "product_id", PRODUCT_ATTRS, "seed_date", product_target)
prod_cdc = spark.table(tbl(RAW_SCHEMA, "bronze_products_cdc"))
for row in prod_cdc.select("change_date").distinct().orderBy("change_date").collect():
    day = row[0]
    n = apply_cdc_day(product_target, prod_cdc.filter(F.col("change_date") == day), "product_id", PRODUCT_ATTRS, "change_date")
    print(f"  product CDC {day}: {n} new/changed rows")

# ============================================================== 04 - GOLD
print("\n=== 04 - Gold ===")

orders = spark.table(tbl(SILVER_SCHEMA, "silver1_orders_clean"))
dim_customer = spark.table(customer_target)
dim_product = spark.table(product_target)

fact_pit = (
    orders.alias("o")
    .join(dim_customer.alias("c"),
          (F.col("o.customer_id") == F.col("c.customer_id")) &
          (F.col("o.order_date") >= F.col("c.effective_start_date")) &
          (F.col("o.order_date") <= F.col("c.effective_end_date")), "left")
    .join(dim_product.alias("p"),
          (F.col("o.product_id") == F.col("p.product_id")) &
          (F.col("o.order_date") >= F.col("p.effective_start_date")) &
          (F.col("o.order_date") <= F.col("p.effective_end_date")), "left")
    .join(dim_store.alias("s"), F.col("o.store_id") == F.col("s.store_id"), "left")
    .select("o.order_id", F.col("c.customer_id_sk").alias("customer_sk"),
            F.col("p.product_id_sk").alias("product_sk"), F.col("s.store_id").alias("store_sk"),
            "o.order_date", "o.quantity", "o.unit_price",
            (F.col("o.quantity") * F.col("o.unit_price")).alias("revenue"),
            F.col("p.category").alias("product_category"), F.col("c.segment").alias("customer_segment"),
            F.col("s.region").alias("store_region"), "o.customer_id", "o.product_id")
)

w_cf = Window.partitionBy("customer_id").orderBy("effective_start_date")
cust_first = (dim_customer.withColumn("rn", F.row_number().over(w_cf)).filter("rn = 1")
              .select("customer_id", F.col("customer_id_sk").alias("customer_sk_fb"), F.col("segment").alias("segment_fb")))
w_pf = Window.partitionBy("product_id").orderBy("effective_start_date")
prod_first = (dim_product.withColumn("rn", F.row_number().over(w_pf)).filter("rn = 1")
              .select("product_id", F.col("product_id_sk").alias("product_sk_fb"), F.col("category").alias("category_fb")))

fact_all = (
    fact_pit.join(cust_first, "customer_id", "left").join(prod_first, "product_id", "left")
    .withColumn("customer_key_is_estimated", F.col("customer_sk").isNull() & F.col("customer_sk_fb").isNotNull())
    .withColumn("product_key_is_estimated", F.col("product_sk").isNull() & F.col("product_sk_fb").isNotNull())
    .withColumn("customer_sk", F.coalesce("customer_sk", "customer_sk_fb"))
    .withColumn("product_sk", F.coalesce("product_sk", "product_sk_fb"))
    .withColumn("customer_segment", F.coalesce("customer_segment", "segment_fb"))
    .withColumn("product_category", F.coalesce("product_category", "category_fb"))
    .drop("customer_sk_fb", "product_sk_fb", "segment_fb", "category_fb")
)

is_resolved = F.col("customer_sk").isNotNull() & F.col("product_sk").isNotNull()
fact_orders = fact_all.filter(is_resolved)
fact_quarantine = fact_all.filter(~is_resolved)
fact_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD_SCHEMA, "fact_orders"))
fact_quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD_SCHEMA, "fact_orders_referential_quarantine"))
print(f"  fact_orders={fact_orders.count()} referential_quarantine={fact_quarantine.count()}")

fact = fact_orders
daily = (fact.groupBy("order_date")
         .agg(F.countDistinct("order_id").alias("total_orders"), F.sum("revenue").alias("total_revenue"),
              F.sum("quantity").alias("total_units"))
         .withColumn("avg_order_value", F.round(F.col("total_revenue") / F.col("total_orders"), 2))
         .orderBy("order_date"))
category = (fact.groupBy("product_category")
            .agg(F.countDistinct("order_id").alias("total_orders"), F.sum("revenue").alias("total_revenue"),
                 F.sum("quantity").alias("total_units")).orderBy(F.desc("total_revenue")))
segment = (fact.groupBy("customer_segment")
           .agg(F.countDistinct("customer_sk").alias("unique_customers"), F.countDistinct("order_id").alias("total_orders"),
                F.sum("revenue").alias("total_revenue")).orderBy(F.desc("total_revenue")))
region = (fact.groupBy("store_region")
          .agg(F.countDistinct("order_id").alias("total_orders"), F.sum("revenue").alias("total_revenue"),
               F.sum("quantity").alias("total_units")).orderBy(F.desc("total_revenue")))

for name, df in [("gold_daily_sales", daily), ("gold_category_sales", category),
                  ("gold_segment_sales", segment), ("gold_region_sales", region)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD_SCHEMA, name))

print("\n=== gold_category_sales ===")
category.show(truncate=False)
print("=== gold_region_sales ===")
region.show(truncate=False)

print(f"\nAll done. Delta tables are on disk under: {WAREHOUSE_DIR}")
print("Query them anytime with: spark.table('retail_demo_gold.gold_daily_sales').show()")
