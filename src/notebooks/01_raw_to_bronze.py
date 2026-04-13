# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Ingestion
# MAGIC Reads raw JSON files from the volume, applies minimal schema, and writes to bronze Delta table.
# MAGIC Tracks processed files to avoid re-processing (idempotent).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "postnl_retail_dev")
dbutils.widgets.text("schema", "monitoring")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw_path = f"/Volumes/{catalog}/{schema}/raw_data/incoming"
bronze_table = f"{catalog}.{schema}.bronze_transactions"
checkpoint_table = f"{catalog}.{schema}.processed_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define schemas

# COMMAND ----------

item_schema = ArrayType(
    StructType(
        [
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("line_total", DoubleType()),
        ]
    )
)

raw_schema = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("store_id", StringType()),
        StructField("store_name", StringType()),
        StructField("city", StringType()),
        StructField("timestamp", StringType()),
        StructField("customer_id", StringType()),
        StructField("payment_method", StringType()),
        StructField("items", item_schema),
        StructField("total_amount", DoubleType()),
        StructField("currency", StringType()),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Track processed files for idempotency

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {checkpoint_table} (
        file_path STRING,
        processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Tracks files already ingested into bronze'
"""
)

processed = set(
    row.file_path for row in spark.table(checkpoint_table).select("file_path").collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest new raw files into bronze

# COMMAND ----------

try:
    files = dbutils.fs.ls(raw_path)
except Exception:
    files = []
    print("No raw files found yet. Skipping ingestion.")

new_files = [f for f in files if f.path not in processed and f.name.endswith(".json")]

if not new_files:
    print("No new files to process.")
    dbutils.notebook.exit("NO_NEW_FILES")

print(f"Processing {len(new_files)} new file(s)...")

# COMMAND ----------

# Read all new JSON files
raw_df = (
    spark.read.schema(raw_schema)
    .json([f.path for f in new_files])
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn(
        "_source_file", F.col("_metadata.file_path")
    )
)

print(f"Read {raw_df.count()} records from {len(new_files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to bronze table

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
        transaction_id STRING,
        store_id STRING,
        store_name STRING,
        city STRING,
        `timestamp` STRING,
        customer_id STRING,
        payment_method STRING,
        items ARRAY<STRUCT<
            product_id: STRING,
            product_name: STRING,
            category: STRING,
            quantity: INT,
            unit_price: DOUBLE,
            line_total: DOUBLE
        >>,
        total_amount DOUBLE,
        currency STRING,
        _ingested_at TIMESTAMP,
        _source_file STRING
    )
    USING DELTA
    COMMENT 'Bronze layer: raw retail transactions with metadata'
    TBLPROPERTIES ('quality' = 'bronze')
"""
)

raw_df.write.mode("append").saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mark files as processed

# COMMAND ----------

for f in new_files:
    escaped_path = f.path.replace("'", "''")
    spark.sql(
        f"""
        INSERT INTO {checkpoint_table}
        VALUES ('{escaped_path}', current_timestamp())
    """
    )

print(f"Marked {len(new_files)} files as processed.")
