-- PostNL Retail Pipeline - Job Run History (last 7 days)
-- Use this query in a DBSQL dashboard or ad-hoc monitoring

SELECT
  j.name AS job_name,
  jr.run_id,
  jr.result_state,
  jr.period_start_time,
  jr.period_end_time,
  ROUND(
    TIMESTAMPDIFF(SECOND, jr.period_start_time, jr.period_end_time) / 60.0, 2
  ) AS duration_minutes,
  jr.run_type
FROM system.lakeflow.job_run_timeline AS jr
JOIN system.lakeflow.jobs AS j
  ON jr.job_id = j.job_id
WHERE j.name LIKE '%PostNL Retail Pipeline%'
  AND jr.period_start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY jr.period_start_time DESC
LIMIT 100;
