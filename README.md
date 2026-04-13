# PostNL Retail Monitoring Pipeline

End-to-end retail data pipeline deployed on Databricks using **serverless compute**, the **medallion architecture**, and **Databricks Asset Bundles (DABs)** for infrastructure-as-code. Includes system table monitoring, cost tracking via resource tags, and automated alerts.

## Architecture

```
┌─────────────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Data Generator  │────▶│   Raw    │────▶│  Bronze  │────▶│  Silver  │────▶│   Gold   │
│  (Synthetic POS) │     │ (Volume) │     │  (Delta) │     │  (Delta) │     │  (Delta) │
└─────────────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
                         JSON files       + metadata       + deduped         + aggregations
                                          + source file    + DQ checked      + hourly sales
                                                           + flattened       + product perf
                                                                            + payment analysis
                                                                            + pipeline health
```

## Pipeline Tasks

The serverless job runs **every hour** with 4 sequential tasks:

| Task | Notebook | Description |
|------|----------|-------------|
| `generate_raw_data` | `00_generate_raw_data.py` | Generates 150-300 synthetic Dutch retail transactions (stroopwafels, gouda, bitterballen, etc.) across 5 stores and writes JSON to a UC Volume |
| `raw_to_bronze` | `01_raw_to_bronze.py` | Ingests new JSON files into a bronze Delta table with ingestion metadata. Tracks processed files for idempotency |
| `bronze_to_silver` | `02_bronze_to_silver.py` | Deduplicates by transaction ID, runs data quality checks using [Databricks Labs DQX](https://github.com/databrickslabs/dqx), quarantines bad records, and flattens line items into a separate table |
| `silver_to_gold` | `03_silver_to_gold.py` | Builds 4 gold tables: hourly sales by store, daily product performance, payment method analysis, and a pipeline health summary |

## Monitoring & Observability

### Data Quality with DQX

The silver layer uses [Databricks Labs DQX](https://github.com/databrickslabs/dqx) for declarative data quality checks. DQX applies row-level rules and splits the data into valid and quarantined DataFrames.

| Check | Criticality | Rule |
|-------|------------|------|
| `transaction_id_not_null` | error | Transaction ID must not be null or empty |
| `store_id_not_null` | error | Store ID must not be null or empty |
| `customer_id_not_null` | error | Customer ID must not be null or empty |
| `timestamp_not_null` | error | Timestamp must not be null or empty |
| `total_amount_positive` | error | Total amount must be greater than zero |
| `items_not_empty` | error | Items array must contain at least one item |
| `payment_method_valid` | warn | Payment method must be one of: card, cash, ideal, contactless, apple_pay |
| `currency_is_eur` | warn | Currency must be EUR |
| `total_amount_in_range` | warn | Total amount must be between 0.01 and 10,000 |

- **error** checks: failing rows are quarantined to `quarantine_transactions`
- **warn** checks: flagged but rows still pass to silver

### Built-in Observability Tables

| Table | Purpose |
|-------|---------|
| `ingestion_log` | Tracks every raw data batch (batch ID, file path, record count, timestamp) |
| `data_quality_log` | Records DQ metrics per run (total, clean, quarantined counts, quarantine rate) |
| `quarantine_transactions` | Rows that failed DQX error-level checks, with `_error` and `_warning` columns for diagnosis |
| `gold_pipeline_health` | Snapshots pipeline state (record counts across layers, avg quarantine rate, last ingestion time) |

### System Table Monitoring Queries

SQL queries in `src/queries/` for use in DBSQL dashboards:

| Query | What It Monitors |
|-------|-----------------|
| `01_job_run_history.sql` | Job run history with duration and result state (7 days) |
| `02_job_cost_by_tag.sql` | Cost breakdown by project tag using `system.billing.usage` and `system.billing.list_prices` |
| `03_job_failure_rate.sql` | Success/failure counts and failure rate percentage (24 hours) |
| `04_task_duration_trends.sql` | Per-task average and max duration trends (7 days) |
| `05_daily_dbu_consumption.sql` | Daily DBU consumption by SKU for tagged resources (30 days) |
| `06_data_freshness.sql` | Minutes since last update for ingestion log, bronze, and silver tables |

### Alerts

| Alert | Trigger | Check Interval |
|-------|---------|----------------|
| **High Job Failure Rate** | Failure rate > 20% in last 24h | Every 15 min |
| **Stale Data** | No new ingestion in 30+ minutes | Every 10 min |
| **High Quarantine Rate** | Data quality quarantine rate > 5% | Every 15 min |
| **Daily Cost Spike** | Estimated daily cost > $50 USD | Every 4 hours |

All alerts send email notifications to the deploying user.

### Lakehouse Monitoring

[Databricks Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html) is enabled on key silver and gold tables to automatically track statistical properties and data quality over time. Each monitor generates two metric tables and an auto-generated dashboard.

| Monitored Table | What It Tracks |
|-----------------|---------------|
| `silver_transactions` | Transaction volume distributions, null rates, payment method cardinality, amount ranges |
| `silver_line_items` | Product quantity and price distributions, category cardinality, line total ranges |
| `gold_hourly_sales_by_store` | Revenue and customer count distributions per store, hourly volume patterns |
| `gold_product_performance` | Units sold and revenue distributions, product count trends |

For each monitored table, Lakehouse Monitoring creates:
- **`<table>_profile_metrics`** — Statistical profiles per column (nulls, distinct counts, min/max, mean, quantiles, distributions)
- **`<table>_drift_metrics`** — Data drift detection between consecutive snapshots (distribution changes, statistical shifts)
- **Auto-generated dashboard** — Visual summary of profile and drift metrics

Monitors run daily at **midnight (Europe/Amsterdam)** on an automated schedule.

## Cost Tracking

Resources are tagged for cost attribution via `system.billing.usage`:

```yaml
tags:
  project: "postnl-retail-monitoring"
  team: "data-engineering"
  cost_center: "postnl-demo"
  environment: "dev"  # or "prod"
```

Use `src/queries/02_job_cost_by_tag.sql` to query costs filtered by these tags.

## Project Structure

```
├── databricks.yml                          # Bundle config with dev/prod targets
├── resources/
│   ├── jobs.yml                            # Serverless job (4 tasks, hourly schedule, tags)
│   └── monitoring.yml                      # 4 SQL alerts
└── src/
    ├── notebooks/
    │   ├── 00_generate_raw_data.py         # Synthetic Dutch retail data generator
    │   ├── 01_raw_to_bronze.py             # Raw JSON → bronze Delta (idempotent)
    │   ├── 02_bronze_to_silver.py          # Dedupe, DQ checks, flatten
    │   └── 03_silver_to_gold.py            # Business aggregations
    └── queries/
        ├── 01_job_run_history.sql
        ├── 02_job_cost_by_tag.sql
        ├── 03_job_failure_rate.sql
        ├── 04_task_duration_trends.sql
        ├── 05_daily_dbu_consumption.sql
        └── 06_data_freshness.sql
```

## Deployment

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.230.0+
- A Databricks workspace with Unity Catalog enabled
- A configured CLI profile (default: `DEFAULT`)

### Deploy

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev (default target)
databricks bundle deploy

# Deploy to production
databricks bundle deploy -t prod

# Trigger a manual run
databricks bundle run retail_pipeline
```

### Configuration

Edit `databricks.yml` to set your catalog and schema:

```yaml
targets:
  dev:
    variables:
      catalog: "your_catalog"
      schema: "your_schema"
```

### Cleanup

```bash
databricks bundle destroy
```

## Data Model

### Bronze: `bronze_transactions`
Raw transactions with ingestion metadata (`_ingested_at`, `_source_file`). Nested `items` array preserved as-is.

### Silver: `silver_transactions` + `silver_line_items`
Deduplicated transactions with parsed timestamps. Line items exploded into a separate table with product-level detail. Records failing DQ checks are quarantined.

### Gold
- **`gold_hourly_sales_by_store`** — Transaction count, revenue, avg value, unique customers per store per hour
- **`gold_product_performance`** — Units sold, revenue, avg price per product per day
- **`gold_payment_analysis`** — Transaction count and revenue by payment method and city per day
- **`gold_pipeline_health`** — Point-in-time snapshot of record counts, quarantine rates, and ingestion recency
