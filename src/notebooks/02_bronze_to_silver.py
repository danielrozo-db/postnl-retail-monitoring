# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleansed & Enriched
# MAGIC Reads bronze data, flattens nested items, deduplicates, applies data quality checks
# MAGIC using **Databricks Labs DQX**, and writes clean, typed records to silver Delta tables.
# MAGIC Quarantined rows are saved to a separate table for investigation.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx import check_funcs
from databricks.sdk import WorkspaceClient

# COMMAND ----------

dbutils.widgets.text("catalog", "postnl_retail_dev")
dbutils.widgets.text("schema", "monitoring")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_transactions"
silver_transactions = f"{catalog}.{schema}.silver_transactions"
silver_line_items = f"{catalog}.{schema}.silver_line_items"
quarantine_table = f"{catalog}.{schema}.quarantine_transactions"
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
# MAGIC ## Define DQX quality checks

# COMMAND ----------

dq_engine = DQEngine(WorkspaceClient())

checks = [
    # Required fields must not be null or empty
    DQRowRule(
        name="transaction_id_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="transaction_id",
    ),
    DQRowRule(
        name="store_id_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="store_id",
    ),
    DQRowRule(
        name="customer_id_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="customer_id",
    ),

    # Timestamp must be valid
    DQRowRule(
        name="timestamp_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="timestamp",
    ),

    # Total amount must be positive
    DQRowRule(
        name="total_amount_positive",
        criticality="error",
        check_func=check_funcs.sql_expression,
        check_func_kwargs={
            "expression": "total_amount > 0",
            "msg": "total_amount must be greater than zero",
        },
    ),

    # Items array must not be empty
    DQRowRule(
        name="items_not_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty_array,
        column="items",
    ),

    # Payment method must be a known value
    DQRowRule(
        name="payment_method_valid",
        criticality="warn",
        check_func=check_funcs.is_in_list,
        column="payment_method",
        check_func_kwargs={
            "allowed": ["card", "cash", "ideal", "contactless", "apple_pay"],
        },
    ),

    # Currency must be EUR
    DQRowRule(
        name="currency_is_eur",
        criticality="warn",
        check_func=check_funcs.is_in_list,
        column="currency",
        check_func_kwargs={"allowed": ["EUR"]},
    ),

    # Total amount should be within a reasonable range for a retail transaction
    DQRowRule(
        name="total_amount_in_range",
        criticality="warn",
        check_func=check_funcs.is_in_range,
        column="total_amount",
        check_func_kwargs={"min_limit": 0.01, "max_limit": 10000},
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply DQX checks and split into valid / quarantine

# COMMAND ----------

total_records = deduped_df.count()

clean_df, quarantine_df = dq_engine.apply_checks_and_split(deduped_df, checks)

clean_count = clean_df.count()
quarantine_count = quarantine_df.count()

print(f"Total: {total_records} | Clean: {clean_count} | Quarantined: {quarantine_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save quarantined rows for investigation

# COMMAND ----------

if quarantine_count > 0:
    quarantine_df.write.mode("append").saveAsTable(quarantine_table)
    print(f"Saved {quarantine_count} quarantined rows to {quarantine_table}")

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
