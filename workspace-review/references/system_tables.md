# Data sources and required access

The review uses whatever the person running it can read. Preflight probes each source and the report
lists anything missing.

| Source | Used for | If missing |
|---|---|---|
| `system.information_schema.{catalogs,schemata,tables,volumes}` | UC inventory | UC section skipped |
| `DESCRIBE DETAIL` / `DESCRIBE HISTORY` / `DESCRIBE TABLE EXTENDED` / `SHOW PARTITIONS` | File counts, sizes, layout, clone + OPTIMIZE history, PO status | Per-table errors listed in the appendix |
| `system.access.table_lineage` | Detecting CTAS migrations from hive_metastore | Migration detection uses history + properties only |
| `system.storage.predictive_optimization_operations_history` | PO compaction activity per table | Causes less certain |
| `SHOW SCHEMAS/TABLES IN hive_metastore` | Remaining Hive tables | Hive section skipped |
| `system.lakeflow.{jobs,job_run_timeline,job_task_run_timeline,pipelines,pipeline_update_timeline}` | Job/pipeline runs and failures | Jobs API (runs only; no pipelines) |
| `system.compute.{clusters,node_timeline}` | Cluster config and utilization | Clusters API (no utilization) |
| `system.billing.{usage,list_prices}` | List-price cost per job, cluster, warehouse | Costs omitted |
| REST: `jobs/list`, `clusters/spark-versions`, `sql/warehouses` | Job settings, available runtimes, warehouses | Those checks skipped |
| REST: `workspace-conf`, `settings/types/*`, `token-management/tokens`, `ip-access-lists`, `global-init-scripts`, `policies/clusters/list`, SCIM `Groups` (admins) | Workspace settings | Shown as unknown |

## Grants needed

- `USE CATALOG` / `USE SCHEMA` / `SELECT` on `system.access`, `system.storage`, `system.billing`, `system.compute`,
  `system.lakeflow` (an account admin must enable the schemas and grant access).
- To see every table: metastore admin, or `USE CATALOG` + `USE SCHEMA` + `SELECT` (or `BROWSE`) on each catalog.
  Tables the caller can't see are simply not counted.
- `DESCRIBE HISTORY` needs `SELECT` on the table.
- Workspace settings, tokens, and the admins group need a **workspace admin**. Jobs, clusters, and warehouses
  show only what the caller can see unless they're an admin.

## Notes

- `system.information_schema` is metastore-wide, not per workspace. Catalogs bound to other workspaces can appear.
- PO history and lineage are not filtered by workspace, because the migration or PO run may have happened in
  another workspace on the same metastore.
- PO `operation_metrics` keys used: `COMPACTION.number_of_compacted_files` and `CLUSTERING.number_of_removed_files`
  (confirmed on a live workspace). Preflight records all keys seen in `ctx.stats["preflight"]["po_metric_keys"]`.
- System tables keep one row per change (jobs, clusters, warehouses, pipelines) or per hourly slice (runs);
  queries take the latest row per ID and group slices by run. `workspace_id` is a string in these tables.
