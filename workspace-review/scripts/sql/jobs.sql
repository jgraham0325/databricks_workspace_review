-- Jobs and pipelines. Columns confirmed against a live workspace (2026-10).
-- system.lakeflow.* tables keep one row per change (jobs, pipelines) or per time slice (runs), so each
-- query collapses them. workspace_id is a string in these tables.
-- Placeholders: {workspace_id}, {activity_window_days}

-- name: jobs_current
-- Latest definition of every job that hasn't been deleted.
SELECT job_id, name, creator_user_name, run_as_user_name, paused, trigger_type, timeout_seconds, create_time
FROM (
  SELECT *, row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs
  WHERE workspace_id = '{workspace_id}'
)
WHERE rn = 1 AND delete_time IS NULL

-- name: job_runs
-- One row per job run started in the activity window. result_state is only set on a run's final slice.
SELECT job_id, run_id,
       min(period_start_time) AS start_time,
       max(period_end_time) AS end_time,
       max(result_state) AS result_state,
       max(termination_code) AS termination_code,
       max(trigger_type) AS trigger_type,
       max(run_duration_seconds) AS run_duration_seconds
FROM system.lakeflow.job_run_timeline
WHERE workspace_id = '{workspace_id}'
  AND run_type = 'JOB_RUN'
  AND period_start_time >= current_timestamp() - INTERVAL {activity_window_days} DAYS
GROUP BY job_id, run_id

-- name: job_task_compute
-- Which compute each job's tasks ran on in the window.
SELECT job_id, c.type AS compute_type, c.cluster_id AS cluster_id, count(DISTINCT job_run_id) AS job_runs
FROM (
  SELECT job_id, job_run_id, explode(compute) AS c
  FROM system.lakeflow.job_task_run_timeline
  WHERE workspace_id = '{workspace_id}'
    AND period_start_time >= current_timestamp() - INTERVAL {activity_window_days} DAYS
)
GROUP BY job_id, c.type, c.cluster_id

-- name: job_cost
-- DBUs and list-price cost per job in the window, with the all-purpose share.
SELECT u.usage_metadata.job_id AS job_id,
       sum(u.usage_quantity) AS dbus,
       sum(u.usage_quantity * p.pricing.default) AS list_cost,
       sum(CASE WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN u.usage_quantity ELSE 0 END) AS all_purpose_dbus,
       sum(CASE WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN u.usage_quantity * p.pricing.default ELSE 0 END)
         AS all_purpose_list_cost,
       max(p.currency_code) AS currency
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.workspace_id = '{workspace_id}'
  AND u.usage_date >= date_sub(current_date(), {activity_window_days})
  AND u.usage_metadata.job_id IS NOT NULL
GROUP BY u.usage_metadata.job_id

-- name: pipeline_updates
-- Latest update per pipeline in the window, with failure counts.
SELECT p.pipeline_id, p.name, p.pipeline_type, p.run_as, u.updates, u.failed_updates, u.last_result_state,
       u.last_update_time
FROM (
  SELECT pipeline_id,
         count(*) AS updates,
         sum(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_updates,
         max_by(result_state, period_end_time) AS last_result_state,
         max(period_end_time) AS last_update_time
  FROM (
    SELECT pipeline_id, update_id, max(result_state) AS result_state, max(period_end_time) AS period_end_time
    FROM system.lakeflow.pipeline_update_timeline
    WHERE workspace_id = '{workspace_id}'
      AND period_start_time >= current_timestamp() - INTERVAL {activity_window_days} DAYS
    GROUP BY pipeline_id, update_id
  )
  GROUP BY pipeline_id
) u
JOIN (
  SELECT pipeline_id, name, pipeline_type, run_as
  FROM (
    SELECT *, row_number() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.pipelines
    WHERE workspace_id = '{workspace_id}'
  )
  WHERE rn = 1 AND delete_time IS NULL
) p ON p.pipeline_id = u.pipeline_id
