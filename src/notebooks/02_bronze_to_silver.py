# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleansed & Enriched
# MAGIC Reads bronze data, flattens nested items, deduplicates, applies data quality checks,
# MAGIC and writes clean, typed records to silver Delta tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "postnl_retail_dev")
dbutils.widgets.text("schema", "monitoring")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_transactions"
silver_transactions = f"{catalog}.{schema}.silver_transactions"
silver_line_items = f"{catalog}.{schema}.silver_line_items"
dq_log_table = f"{catalog}.{schema}.data_quality_log"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read bronze and deduplicate

# COMMAND ----------

bronze_df = spark.table(bronze_table)

# Deduplicate by transaction_id, keeping the latest ingestion
window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())

deduped_df = (
    bronze_df.withColumn("_row_num", F.row_number().over(window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

total_records = deduped_df.count()

# Flag records with quality issues
dq_df = deduped_df.withColumn(
    "_dq_issues",
    F.array_remove(
        F.array(
            F.when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id")),
            F.when(F.col("total_amount") <= 0, F.lit("invalid_total_amount")),
            F.when(F.col("store_id").isNull(), F.lit("missing_store_id")),
            F.when(
                F.col("timestamp").isNull()
                | F.to_timestamp("timestamp").isNull(),
                F.lit("invalid_timestamp"),
            ),
            F.when(F.size("items") == 0, F.lit("empty_items")),
        ),
        None,
    ),
)

clean_df = dq_df.filter(F.size("_dq_issues") == 0)
quarantine_df = dq_df.filter(F.size("_dq_issues") > 0)

clean_count = clean_df.count()
quarantine_count = quarantine_df.count()
print(
    f"Total: {total_records} | Clean: {clean_count} | Quarantined: {quarantine_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log data quality metrics

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {dq_log_table} (
        check_timestamp TIMESTAMP,
        layer STRING,
        total_records LONG,
        clean_records LONG,
        quarantined_records LONG,
        quarantine_rate DOUBLE
    )
    USING DELTA
    COMMENT 'Data quality metrics per pipeline run'
"""
)

quarantine_rate = round(quarantine_count / max(total_records, 1) * 100, 2)

spark.sql(
    f"""
    INSERT INTO {dq_log_table}
    VALUES (
        current_timestamp(), 'silver',
        {total_records}, {clean_count}, {quarantine_count}, {quarantine_rate}
    )
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build silver transactions table

# COMMAND ----------

silver_txn_df = (
    clean_df.select(
        "transaction_id",
        "store_id",
        "store_name",
        "city",
        F.to_timestamp("timestamp").alias("transaction_ts"),
        "customer_id",
        "payment_method",
        "total_amount",
        "currency",
        F.size("items").alias("item_count"),
        "_ingested_at",
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {silver_transactions} (
        transaction_id STRING,
        store_id STRING,
        store_name STRING,
        city STRING,
        transaction_ts TIMESTAMP,
        customer_id STRING,
        payment_method STRING,
        total_amount DOUBLE,
        currency STRING,
        item_count INT,
        _ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Silver layer: cleansed and deduplicated transactions'
    TBLPROPERTIES ('quality' = 'silver')
"""
)

# Merge to handle re-runs idempotently
silver_txn_df.createOrReplaceTempView("new_silver_txn")

spark.sql(
    f"""
    MERGE INTO {silver_transactions} t
    USING new_silver_txn s
    ON t.transaction_id = s.transaction_id
    WHEN NOT MATCHED THEN INSERT *
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build silver line items table (flattened)

# COMMAND ----------

silver_items_df = (
    clean_df.select(
        "transaction_id",
        "store_id",
        "city",
        F.to_timestamp("timestamp").alias("transaction_ts"),
        F.explode("items").alias("item"),
    )
    .select(
        "transaction_id",
        "store_id",
        "city",
        "transaction_ts",
        F.col("item.product_id").alias("product_id"),
        F.col("item.product_name").alias("product_name"),
        F.col("item.category").alias("category"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        F.col("item.line_total").alias("line_total"),
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {silver_line_items} (
        transaction_id STRING,
        store_id STRING,
        city STRING,
        transaction_ts TIMESTAMP,
        product_id STRING,
        product_name STRING,
        category STRING,
        quantity INT,
        unit_price DOUBLE,
        line_total DOUBLE
    )
    USING DELTA
    COMMENT 'Silver layer: flattened line items per transaction'
    TBLPROPERTIES ('quality' = 'silver')
"""
)

silver_items_df.createOrReplaceTempView("new_silver_items")

spark.sql(
    f"""
    MERGE INTO {silver_line_items} t
    USING new_silver_items s
    ON t.transaction_id = s.transaction_id
       AND t.product_id = s.product_id
    WHEN NOT MATCHED THEN INSERT *
"""
)

print("Silver layer updated successfully.")
