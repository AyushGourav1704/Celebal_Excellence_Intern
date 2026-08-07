# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Delta Lake Features Demo
# MAGIC Table history, time travel, and schema evolution — the three features the
# MAGIC guide asks you to demonstrate explicitly.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## Table history
# MAGIC Every write is a numbered, auditable transaction.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG_PREFIX}{SILVER_SCHEMA}.dim_customer_scd2"))

# COMMAND ----------

# MAGIC %md ## Time travel
# MAGIC Query the dimension as it looked at an earlier version or timestamp — useful
# MAGIC for proving your SCD2 logic actually changed something, or for debugging.

# COMMAND ----------

# By version number
display(spark.sql(f"SELECT * FROM {CATALOG_PREFIX}{SILVER_SCHEMA}.dim_customer_scd2 VERSION AS OF 0 LIMIT 5"))

# COMMAND ----------

# By timestamp (adjust to a real timestamp from the DESCRIBE HISTORY output above)
# display(spark.sql(f"""
#   SELECT * FROM {CATALOG_PREFIX}{SILVER_SCHEMA}.dim_customer_scd2
#   TIMESTAMP AS OF '2026-04-23T00:00:00.000+00:00'
#   LIMIT 5
# """))

# COMMAND ----------

# MAGIC %md ## Schema evolution
# MAGIC `coupon_code` first appears on day `2026-04-26`. Confirm the Bronze orders
# MAGIC tables picked it up automatically (batch orders + earlier incremental days
# MAGIC won't have real values for it — that's expected).

# COMMAND ----------

spark.table(f"{CATALOG_PREFIX}{RAW_SCHEMA}.bronze_orders_incremental").printSchema()

# COMMAND ----------

display(spark.sql(f"""
  SELECT load_type, coupon_code IS NOT NULL AS has_coupon, count(*) AS n
  FROM {CATALOG_PREFIX}{RAW_SCHEMA}.bronze_orders_incremental
  GROUP BY load_type, has_coupon
"""))
