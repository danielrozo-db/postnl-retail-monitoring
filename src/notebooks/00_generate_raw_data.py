# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Data Generator - PostNL Retail Shop
# MAGIC Generates fabricated retail transaction data simulating incoming POS data.
# MAGIC Writes raw JSON files to a Unity Catalog volume.

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "postnl_retail_dev")
dbutils.widgets.text("schema", "monitoring")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw_path = f"/Volumes/{catalog}/{schema}/raw_data/incoming"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure infrastructure exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data
    COMMENT 'Raw incoming retail data'
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate synthetic retail data

# COMMAND ----------

PRODUCTS = [
    {"id": "P001", "name": "Stroopwafels", "category": "Snacks", "base_price": 3.49},
    {"id": "P002", "name": "Gouda Cheese", "category": "Dairy", "base_price": 5.99},
    {"id": "P003", "name": "Hagelslag", "category": "Breakfast", "base_price": 2.79},
    {"id": "P004", "name": "Drop Licorice", "category": "Candy", "base_price": 1.99},
    {"id": "P005", "name": "Pindakaas", "category": "Spreads", "base_price": 3.29},
    {"id": "P006", "name": "Frikandel", "category": "Frozen", "base_price": 4.49},
    {"id": "P007", "name": "Rookworst", "category": "Meat", "base_price": 3.99},
    {"id": "P008", "name": "Appelmoes", "category": "Condiments", "base_price": 1.89},
    {"id": "P009", "name": "Bitterballen", "category": "Frozen", "base_price": 5.49},
    {"id": "P010", "name": "Vla Pudding", "category": "Dairy", "base_price": 2.49},
    {"id": "P011", "name": "Kroket", "category": "Frozen", "base_price": 3.79},
    {"id": "P012", "name": "Speculaas", "category": "Cookies", "base_price": 2.99},
    {"id": "P013", "name": "Ontbijtkoek", "category": "Bread", "base_price": 2.59},
    {"id": "P014", "name": "Pannenkoekenmix", "category": "Baking", "base_price": 1.99},
    {"id": "P015", "name": "Chocomel", "category": "Beverages", "base_price": 1.79},
]

STORES = [
    {"id": "S001", "name": "Amsterdam Centraal", "city": "Amsterdam"},
    {"id": "S002", "name": "Rotterdam Zuid", "city": "Rotterdam"},
    {"id": "S003", "name": "Den Haag Centrum", "city": "Den Haag"},
    {"id": "S004", "name": "Utrecht Station", "city": "Utrecht"},
    {"id": "S005", "name": "Eindhoven Airport", "city": "Eindhoven"},
]

PAYMENT_METHODS = ["card", "cash", "ideal", "contactless", "apple_pay"]


def generate_batch(batch_size: int = 200) -> list[dict]:
    """Generate a batch of retail transactions."""
    now = datetime.now()
    records = []

    for _ in range(batch_size):
        store = random.choice(STORES)
        num_items = random.randint(1, 6)
        items = random.choices(PRODUCTS, k=num_items)

        line_items = []
        total = 0.0
        for item in items:
            qty = random.randint(1, 4)
            price = round(item["base_price"] * random.uniform(0.9, 1.1), 2)
            line_total = round(price * qty, 2)
            total += line_total
            line_items.append(
                {
                    "product_id": item["id"],
                    "product_name": item["name"],
                    "category": item["category"],
                    "quantity": qty,
                    "unit_price": price,
                    "line_total": line_total,
                }
            )

        record = {
            "transaction_id": str(uuid.uuid4()),
            "store_id": store["id"],
            "store_name": store["name"],
            "city": store["city"],
            "timestamp": (
                now - timedelta(minutes=random.randint(0, 10))
            ).isoformat(),
            "customer_id": f"C{random.randint(10000, 99999)}",
            "payment_method": random.choice(PAYMENT_METHODS),
            "items": line_items,
            "total_amount": round(total, 2),
            "currency": "EUR",
        }
        records.append(record)

    return records


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write raw data to volume

# COMMAND ----------

batch = generate_batch(batch_size=random.randint(150, 300))
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{uuid.uuid4().hex[:8]}"

# Write as JSON to the raw volume
json_content = "\n".join(json.dumps(r) for r in batch)
file_path = f"{raw_path}/transactions_{batch_id}.json"

dbutils.fs.mkdirs(raw_path)
dbutils.fs.put(file_path, json_content, overwrite=True)

print(f"Generated {len(batch)} transactions -> {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log batch metadata for observability

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingestion_log (
        batch_id STRING,
        file_path STRING,
        record_count INT,
        generated_at TIMESTAMP,
        status STRING
    )
    USING DELTA
    COMMENT 'Tracks each raw data batch ingested'
"""
)

spark.sql(
    f"""
    INSERT INTO {catalog}.{schema}.ingestion_log
    VALUES ('{batch_id}', '{file_path}', {len(batch)}, current_timestamp(), 'SUCCESS')
"""
)

print(f"Batch {batch_id} logged successfully.")
