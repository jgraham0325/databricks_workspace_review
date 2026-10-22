-- Unity Catalog inventory. information_schema only returns objects the caller has some
-- privilege on, so results depend on who runs the review (noted in the report).
-- Placeholders: {system_catalog} (default `system`); {catalog_filter} keeps only in-scope catalogs
-- (include list minus exclusions); {schema_filter} keeps only include_schemas (TRUE if unset).
-- Each query is rendered with the column that holds its catalog name (see uc_assets.CATALOG_COLUMNS)
-- and, where it applies, the schema column (see uc_assets.SCHEMA_COLUMNS).

-- name: catalogs
SELECT catalog_name, catalog_owner, comment, created, created_by, last_altered
FROM {system_catalog}.information_schema.catalogs
WHERE {catalog_filter}

-- name: schemas
SELECT catalog_name, schema_name, schema_owner, comment, created, last_altered
FROM {system_catalog}.information_schema.schemata
WHERE {catalog_filter}
  AND {schema_filter}
  AND schema_name <> 'information_schema'

-- name: tables
SELECT table_catalog, table_schema, table_name, table_type, data_source_format,
       table_owner, comment, created, created_by, last_altered
FROM {system_catalog}.information_schema.tables
WHERE {catalog_filter}
  AND {schema_filter}
  AND table_schema <> 'information_schema'

-- name: volumes
SELECT volume_catalog, volume_schema, volume_name, volume_type, volume_owner, comment, created
FROM {system_catalog}.information_schema.volumes
WHERE {catalog_filter}
  AND {schema_filter}
  AND volume_schema <> 'information_schema'

-- name: hive_lineage_targets
-- UC tables that read from a hive_metastore table (catches CTAS-style migrations).
-- Not filtered to the workspace: the migration may have run from any workspace on the metastore.
SELECT target_table_full_name,
       array_join(collect_set(source_table_full_name), ', ') AS hive_sources,
       min(event_time) AS first_seen
FROM {system_catalog}.access.table_lineage
WHERE source_table_full_name LIKE 'hive_metastore.%'
  AND target_table_full_name IS NOT NULL
  AND target_table_full_name NOT LIKE 'hive_metastore.%'
  AND {catalog_filter}
  AND {schema_filter}
GROUP BY target_table_full_name

-- name: po_activity
-- Successful predictive optimization operations per table, all retained history
-- (not limited to the activity window: the migration may be older than that).
-- Metric keys confirmed on a live workspace (2026-10): COMPACTION.number_of_compacted_files,
-- CLUSTERING.number_of_removed_files. Aggregated here because map columns can't be converted to pandas.
SELECT catalog_name, schema_name, table_name, operation_type,
       count(*) AS runs,
       min(start_time) AS first_run,
       max(start_time) AS last_run,
       sum(usage_quantity) AS dbus,
       sum(coalesce(try_cast(operation_metrics['number_of_compacted_files'] AS BIGINT),
                    try_cast(operation_metrics['number_of_removed_files'] AS BIGINT))) AS files_rewritten
FROM {system_catalog}.storage.predictive_optimization_operations_history
WHERE upper(operation_status) LIKE 'SUCCESS%'
  AND {catalog_filter}
  AND {schema_filter}
GROUP BY catalog_name, schema_name, table_name, operation_type
