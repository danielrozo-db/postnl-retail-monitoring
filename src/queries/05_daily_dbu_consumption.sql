-- PostNL Retail Pipeline - Daily DBU Consumption (last 30 days)

SELECT
  u.usage_date,
  u.sku_name,
  ROUND(SUM(u.usage_quantity), 4) AS total_dbus
FROM system.billing.usage AS u
WHERE u.custom_tags.project = 'postnl-retail-monitoring'
  AND u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY u.usage_date, u.sku_name
ORDER BY u.usage_date DESC;
