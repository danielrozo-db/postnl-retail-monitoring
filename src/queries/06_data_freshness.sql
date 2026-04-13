-- PostNL Retail Pipeline - Data Freshness Check
-- Replace ${catalog} and ${schema} with your target values

SELECT
  'ingestion_log' AS table_name,
  MAX(generated_at) AS last_updated,
  TIMESTAMPDIFF(MINUTE, MAX(generated_at), CURRENT_TIMESTAMP()) AS minutes_since_update
FROM ${catalog}.${schema}.ingestion_log
UNION ALL
SELECT
  'bronze_transactions',
  MAX(_ingested_at),
  TIMESTAMPDIFF(MINUTE, MAX(_ingested_at), CURRENT_TIMESTAMP())
FROM ${catalog}.${schema}.bronze_transactions
UNION ALL
SELECT
  'silver_transactions',
  MAX(_ingested_at),
  TIMESTAMPDIFF(MINUTE, MAX(_ingested_at), CURRENT_TIMESTAMP())
FROM ${catalog}.${schema}.silver_transactions;
