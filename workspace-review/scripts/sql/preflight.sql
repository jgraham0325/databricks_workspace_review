-- Each probe reads zero rows; it only proves the caller can SELECT from the source.
-- Probe names map to keys in ctx.sources.
-- Placeholder: {system_catalog} (default `system`).

-- name: information_schema
SELECT 1 FROM {system_catalog}.information_schema.tables LIMIT 0

-- name: billing
SELECT 1 FROM {system_catalog}.billing.usage LIMIT 0

-- name: list_prices
SELECT 1 FROM {system_catalog}.billing.list_prices LIMIT 0

-- name: compute
SELECT 1 FROM {system_catalog}.compute.clusters LIMIT 0

-- name: node_timeline
SELECT 1 FROM {system_catalog}.compute.node_timeline LIMIT 0

-- name: warehouses
SELECT 1 FROM {system_catalog}.compute.warehouses LIMIT 0

-- name: lakeflow
SELECT 1 FROM {system_catalog}.lakeflow.jobs LIMIT 0

-- name: table_lineage
SELECT 1 FROM {system_catalog}.access.table_lineage LIMIT 0

-- name: predictive_optimization
SELECT 1 FROM {system_catalog}.storage.predictive_optimization_operations_history LIMIT 0

-- name: hive_metastore
SHOW SCHEMAS IN hive_metastore
