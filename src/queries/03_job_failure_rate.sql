-- PostNL Retail Pipeline - Job Failure Rate (last 24h)

SELECT
  j.name AS job_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN jr.result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
  SUM(CASE WHEN jr.result_state != 'SUCCESS' THEN 1 ELSE 0 END) AS failures,
  ROUND(
    SUM(CASE WHEN jr.result_state != 'SUCCESS' THEN 1 ELSE 0 END)
    * 100.0 / COUNT(*), 2
  ) AS failure_rate_pct
FROM system.lakeflow.job_run_timeline AS jr
JOIN system.lakeflow.jobs AS j
  ON jr.job_id = j.job_id
WHERE j.name LIKE '%PostNL Retail Pipeline%'
  AND jr.period_start_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
  AND jr.result_state IS NOT NULL
GROUP BY j.name;
