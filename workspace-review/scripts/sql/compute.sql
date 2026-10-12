-- Compute: clusters, utilization, warehouses, and cost. Columns confirmed against a live workspace (2026-10).
-- system.compute.clusters / warehouses keep one row per change; queries take the latest row.
-- Placeholders: {workspace_id}, {activity_window_days}

-- name: clusters_current
-- Latest config of every cluster that hasn't been deleted (all sources: UI, API, JOB, PIPELINE, ...).
SELECT cluster_id, cluster_name, owned_by, cluster_source, dbr_version, data_security_mode, policy_id,
       auto_termination_minutes, worker_count, min_autoscale_workers, max_autoscale_workers,
       driver_node_type, worker_node_type, tags, create_time, change_time
FROM (
  SELECT *, row_number() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) AS rn
  FROM system.compute.clusters
  WHERE workspace_id = '{workspace_id}'
)
WHERE rn = 1 AND delete_time IS NULL

-- name: cluster_utilization
-- Per cluster, over the window: node-hours, driver uptime hours, average CPU and memory.
SELECT cluster_id,
       sum(timestampdiff(SECOND, start_time, end_time)) / 3600.0 AS node_hours,
       sum(CASE WHEN driver THEN timestampdiff(SECOND, start_time, end_time) ELSE 0 END) / 3600.0
         AS driver_hours,
       avg(cpu_user_percent + cpu_system_percent) AS avg_cpu_pct,
       avg(mem_used_percent) AS avg_mem_pct,
       min(start_time) AS first_seen,
       max(end_time) AS last_seen
FROM system.compute.node_timeline
WHERE workspace_id = '{workspace_id}'
  AND start_time >= current_timestamp() - INTERVAL {activity_window_days} DAYS
GROUP BY cluster_id

-- name: compute_cost
-- DBUs and list-price cost per cluster and per warehouse in the window.
SELECT coalesce(u.usage_metadata.cluster_id, u.usage_metadata.warehouse_id) AS compute_id,
       CASE WHEN u.usage_metadata.cluster_id IS NOT NULL THEN 'cluster' ELSE 'warehouse' END AS compute_kind,
       sum(u.usage_quantity) AS dbus,
       sum(u.usage_quantity * p.pricing.default) AS list_cost,
       max(p.currency_code) AS currency
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.workspace_id = '{workspace_id}'
  AND u.usage_date >= date_sub(current_date(), {activity_window_days})
  AND coalesce(u.usage_metadata.cluster_id, u.usage_metadata.warehouse_id) IS NOT NULL
GROUP BY 1, 2

-- name: warehouses_current
-- Fallback when the SQL Warehouses API isn't available.
SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_channel, warehouse_size, min_clusters,
       max_clusters, auto_stop_minutes, tags, created_by
FROM (
  SELECT *, row_number() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS rn
  FROM system.compute.warehouses
  WHERE workspace_id = '{workspace_id}'
)
WHERE rn = 1 AND delete_time IS NULL
