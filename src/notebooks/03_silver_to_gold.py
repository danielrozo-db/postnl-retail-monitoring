# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations
# MAGIC Builds curated, business-ready aggregate tables from silver data.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "postnl_retail_dev")
dbutils.widgets.text("schema", "monitoring")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver_transactions = f"{catalog}.{schema}.silver_transactions"
silver_line_items = f"{catalog}.{schema}.silver_line_items"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Hourly Sales by Store

# COMMAND ----------

gold_hourly_store = f"{catalog}.{schema}.gold_hourly_sales_by_store"

hourly_df = (
    spark.table(silver_transactions)
    .withColumn("sale_hour", F.date_trunc("hour", "transaction_ts"))
    .groupBy("sale_hour", "store_id", "store_name", "city")
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_transaction_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {gold_hourly_store} (
        sale_hour TIMESTAMP,
        store_id STRING,
        store_name STRING,
        city STRING,
        transaction_count LONG,
        total_revenue DOUBLE,
        avg_transaction_value DOUBLE,
        unique_customers LONG
    )
    USING DELTA
    COMMENT 'Gold layer: hourly sales aggregated by store'
    TBLPROPERTIES ('quality' = 'gold')
"""
)

# Full refresh for gold aggregates
hourly_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    gold_hourly_store
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Product Performance

# COMMAND ----------

gold_product_perf = f"{catalog}.{schema}.gold_product_performance"

product_df = (
    spark.table(silver_line_items)
    .withColumn("sale_date", F.to_date("transaction_ts"))
    .groupBy("sale_date", "product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("total_revenue"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("transaction_id").alias("transaction_count"),
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {gold_product_perf} (
        sale_date DATE,
        product_id STRING,
        product_name STRING,
        category STRING,
        units_sold LONG,
        total_revenue DOUBLE,
        avg_unit_price DOUBLE,
        transaction_count LONG
    )
    USING DELTA
    COMMENT 'Gold layer: daily product performance metrics'
    TBLPROPERTIES ('quality' = 'gold')
"""
)

product_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    gold_product_perf
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Payment Method Analysis

# COMMAND ----------

gold_payment = f"{catalog}.{schema}.gold_payment_analysis"

payment_df = (
    spark.table(silver_transactions)
    .withColumn("sale_date", F.to_date("transaction_ts"))
    .groupBy("sale_date", "payment_method", "city")
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_transaction_value"),
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {gold_payment} (
        sale_date DATE,
        payment_method STRING,
        city STRING,
        transaction_count LONG,
        total_revenue DOUBLE,
        avg_transaction_value DOUBLE
    )
    USING DELTA
    COMMENT 'Gold layer: payment method analysis by city'
    TBLPROPERTIES ('quality' = 'gold')
"""
)

payment_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    gold_payment
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Pipeline Health Summary

# COMMAND ----------

gold_pipeline_health = f"{catalog}.{schema}.gold_pipeline_health"

dq_log_table = f"{catalog}.{schema}.data_quality_log"
ingestion_log = f"{catalog}.{schema}.ingestion_log"

health_df = spark.sql(
    f"""
    SELECT
        current_timestamp() AS snapshot_ts,
        (SELECT COUNT(*) FROM {catalog}.{schema}.bronze_transactions) AS bronze_record_count,
        (SELECT COUNT(*) FROM {silver_transactions}) AS silver_txn_count,
        (SELECT COUNT(*) FROM {silver_line_items}) AS silver_items_count,
        (SELECT COALESCE(AVG(quarantine_rate), 0) FROM {dq_log_table}) AS avg_quarantine_rate_pct,
        (SELECT COUNT(*) FROM {ingestion_log} WHERE status = 'SUCCESS') AS successful_batches,
        (SELECT MAX(generated_at) FROM {ingestion_log}) AS last_ingestion_ts
"""
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {gold_pipeline_health} (
        snapshot_ts TIMESTAMP,
        bronze_record_count LONG,
        silver_txn_count LONG,
        silver_items_count LONG,
        avg_quarantine_rate_pct DOUBLE,
        successful_batches LONG,
        last_ingestion_ts TIMESTAMP
    )
    USING DELTA
    COMMENT 'Gold layer: pipeline health and data quality summary'
    TBLPROPERTIES ('quality' = 'gold')
"""
)

health_df.write.mode("append").saveAsTable(gold_pipeline_health)

print("Gold layer updated successfully.")
