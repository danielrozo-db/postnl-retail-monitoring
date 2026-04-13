-- PostNL Retail Pipeline - Task Duration Trends (last 7 days)

SELECT
  jrt.task_key,
  DATE_TRUNC('hour', jrt.start_time) AS hour_bucket,
  ROUND(AVG(
    TIMESTAMPDIFF(SECOND, jrt.start_time, jrt.end_time)
  ), 1) AS avg_duration_seconds,
  ROUND(MAX(
    TIMESTAMPDIFF(SECOND, jrt.start_time, jrt.end_time)
  ), 1) AS max_duration_seconds,
  COUNT(*) AS run_count
FROM system.lakeflow.job_task_run_timeline AS jrt
JOIN system.lakeflow.jobs AS j
  ON jrt.job_id = j.job_id
WHERE j.name LIKE '%PostNL Retail Pipeline%'
  AND jrt.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY jrt.task_key, DATE_TRUNC('hour', jrt.start_time)
ORDER BY hour_bucket DESC, jrt.task_key;
