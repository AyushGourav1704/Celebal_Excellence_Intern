# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Stage 2: SCD Type 2 Dimensions
# MAGIC Builds `dim_customer_scd2` and `dim_product_scd2` and applies CDC feeds day by
# MAGIC day using the two-step MERGE pattern (expire, then insert). Surrogate key is a
# MAGIC hash of `business_key + effective_start_date`, per the guide's hint — that
# MAGIC keeps it deterministic and reproducible (no dependency on insert order).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

INITIAL_LOAD_DATE = "2020-01-01"   # effective_start_date used for the initial batch seed
FAR_FUTURE = "9999-12-31"

# COMMAND ----------

# MAGIC %md ## Generic SCD2 helpers

# COMMAND ----------

def build_hash(df, attr_cols):
    return df.withColumn("hash_value", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in attr_cols]), 256))


def seed_scd2_table(source_df, business_key, attr_cols, start_date_col, target_full_name):
    """Initial load: one open row per distinct current business key."""
    hashed = build_hash(source_df, attr_cols)
    seeded = (
        hashed
        .withColumn("effective_start_date", F.coalesce(F.col(start_date_col), F.lit(INITIAL_LOAD_DATE)).cast("date"))
        .withColumn("effective_end_date", F.lit(FAR_FUTURE).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(f"{business_key}_sk",
                    F.sha2(F.concat_ws("||", F.col(business_key), F.col("effective_start_date").cast("string")), 256))
        .select(f"{business_key}_sk", business_key, *attr_cols,
                "effective_start_date", "effective_end_date", "is_current", "hash_value")
    )
    (seeded.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(target_full_name))
    return spark.table(target_full_name).count()


def apply_scd2_cdc_for_day(target_full_name, cdc_day_df, business_key, attr_cols, change_date_col):
    """One day's CDC batch -> two-step MERGE (expire changed actives, insert new versions)."""
    src = build_hash(cdc_day_df, attr_cols).withColumn(change_date_col, F.col(change_date_col).cast("date"))
    # collapse multiple changes for the same key on the same day to the last one
    src = (
        src.withColumn("rn", F.row_number().over(
            __import__("pyspark").sql.window.Window
            .partitionBy(business_key).orderBy(F.monotonically_increasing_id().desc())))
        .filter("rn = 1").drop("rn")
    )

    target = DeltaTable.forName(spark, target_full_name)

    # Step 1: expire the active row for any business key whose incoming hash differs
    (target.alias("t")
     .merge(src.alias("s"), f"t.{business_key} = s.{business_key} AND t.is_current = true")
     .whenMatchedUpdate(
         condition="t.hash_value <> s.hash_value",
         set={
             "is_current": "false",
             "effective_end_date": f"date_sub(s.{change_date_col}, 1)",
         },
     )
     .execute())

    # Step 2: insert new open rows for brand-new keys AND keys we just expired above
    current_actives = target.toDF().filter("is_current = true").select(business_key, "hash_value")
    to_insert = src.join(current_actives, on=[business_key, "hash_value"], how="left_anti")

    if to_insert.take(1):
        new_rows = (
            to_insert
            .withColumnRenamed(change_date_col, "effective_start_date")
            .withColumn("effective_end_date", F.lit(FAR_FUTURE).cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn(f"{business_key}_sk",
                        F.sha2(F.concat_ws("||", F.col(business_key), F.col("effective_start_date").cast("string")), 256))
            .select(f"{business_key}_sk", business_key, *attr_cols,
                    "effective_start_date", "effective_end_date", "is_current", "hash_value")
        )
        new_rows.write.format("delta").mode("append").saveAsTable(target_full_name)

    return to_insert.count()

# COMMAND ----------

# MAGIC %md ## Customer dimension

# COMMAND ----------

CUSTOMER_ATTRS = ["customer_name", "email", "city", "segment"]
customer_target = f"{CATALOG_PREFIX}{SILVER_SCHEMA}.dim_customer_scd2"

customers_clean = spark.table(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_customers_clean")
n = seed_scd2_table(customers_clean, "customer_id", CUSTOMER_ATTRS, "signup_date", customer_target)
print("seeded dim_customer_scd2 with", n, "rows")

customers_cdc = spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_customers_cdc")
cdc_days = [r[0] for r in customers_cdc.select("change_date").distinct().orderBy("change_date").collect()]
for day in cdc_days:
    day_df = customers_cdc.filter(F.col("change_date") == day)
    inserted = apply_scd2_cdc_for_day(customer_target, day_df, "customer_id", CUSTOMER_ATTRS, "change_date")
    print(f"customer CDC {day}: {inserted} new/changed dimension rows")

# COMMAND ----------

# MAGIC %md ## Product dimension

# COMMAND ----------

PRODUCT_ATTRS = ["product_name", "category", "unit_price"]
product_target = f"{CATALOG_PREFIX}{SILVER_SCHEMA}.dim_product_scd2"

products_clean = spark.table(f"{CATALOG_PREFIX}{SILVER_SCHEMA}.silver1_products_clean")
products_clean_seed = products_clean.withColumn("seed_date", F.lit(INITIAL_LOAD_DATE))
n = seed_scd2_table(products_clean_seed, "product_id", PRODUCT_ATTRS, "seed_date", product_target)
print("seeded dim_product_scd2 with", n, "rows")

products_cdc = spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_products_cdc")
cdc_days = [r[0] for r in products_cdc.select("change_date").distinct().orderBy("change_date").collect()]
for day in cdc_days:
    day_df = products_cdc.filter(F.col("change_date") == day)
    inserted = apply_scd2_cdc_for_day(product_target, day_df, "product_id", PRODUCT_ATTRS, "change_date")
    print(f"product CDC {day}: {inserted} new/changed dimension rows")

# COMMAND ----------

# MAGIC %md ## Sanity checks

# COMMAND ----------

display(spark.sql(f"""
  SELECT customer_id, count(*) AS versions
  FROM {customer_target}
  GROUP BY customer_id
  HAVING count(*) > 1
  ORDER BY versions DESC
  LIMIT 10
"""))

display(spark.sql(f"""
  SELECT * FROM {customer_target} WHERE is_current = true LIMIT 5
"""))
