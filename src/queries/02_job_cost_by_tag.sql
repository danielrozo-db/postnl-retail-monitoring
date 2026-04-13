-- PostNL Retail Pipeline - Cost Tracking by Tag (last 30 days)
-- Tracks DBU consumption and estimated cost for tagged resources

SELECT
  u.usage_date,
  u.custom_tags.project AS project_tag,
  u.custom_tags.cost_center AS cost_center,
  u.sku_name,
  ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
  ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS estimated_cost_usd
FROM system.billing.usage AS u
LEFT JOIN system.billing.list_prices AS lp
  ON u.cloud = lp.cloud
  AND u.sku_name = lp.sku_name
  AND u.usage_start_time BETWEEN lp.price_start_time
    AND COALESCE(lp.price_end_time, TIMESTAMP '2099-12-31')
WHERE u.custom_tags.project = 'postnl-retail-monitoring'
  AND u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY ALL
ORDER BY u.usage_date DESC;
