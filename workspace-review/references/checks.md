# Checks

All thresholds live in `scripts/review/config.py` and can be overridden per run. Set `system_catalog` when
system tables are exposed through a views catalog instead of `system`.
Severity: 🔴 HIGH · 🟠 MEDIUM · 🟡 LOW.

**Scope vs activity window.** `include_areas` chooses sections (`uc`, `jobs`, `compute`, `settings`; empty = all).
`include_catalogs`, `include_schemas`, and exclusions decide *which* objects are reviewed in Unity Catalog. The activity
window (`activity_window_days`) only sets how far back to look for recent activity (maintenance runs; later job
runs and spend). Tables are never filtered by creation date, and clone/OPTIMIZE history and PO history are read
in full so that old migrations are still detected.

## Adding a check

Checks are explicit `CheckSpec` entries in their area module; no file scanning or plugin installation is involved.

1. Reuse or extend the area's collector so the required raw data is stored in `ctx.stats`.
2. Add one evaluator that accepts `ReviewContext` and emits at most one `Finding` check ID.
3. Add its `CheckSpec` to the module's `CHECKS` tuple, including config keys used by the evaluator.
4. Add focused canned-data tests and document the check ID and threshold here.
5. Run `python3 -m pytest`. Registry tests enforce unique IDs, valid metadata, and documentation coverage.

Normal checks in an existing area do not require report or template changes: findings, evidence CSVs, remediation,
and the drill-down notebook are rendered generically.

## Health score (per area)

Each area starts at 100. Every finding in the area subtracts a weight by severity
(HIGH 20, MEDIUM 8, LOW 3), floored at 0. A finding covers all affected objects for one check, so 300 tables
with small files cost the same as one: the evidence table shows the scale.

| Score | Band |
|---|---|
| 85–100 | Good |
| 60–84 | Needs attention |
| 0–59 | At risk |

## Unity Catalog data (`uc`)

### MIG-BACKLOG: migrated tables still carrying small files
- **Flags:** a table migrated from `hive_metastore` that meets the small-file condition (UC-SMALL-FILES).
- **Migration detected by**, in order: a `CLONE` from `hive_metastore.*` in `DESCRIBE HISTORY`; a UCX
  `upgraded_from` table property; a `hive_metastore.*` source in `system.access.table_lineage`.
  History is kept 30 days by default, so for older migrations the second and third signals matter.
- **Severity:** HIGH if ≥ 100,000 files; or if OPTIMIZE / predictive optimization has run but has rewritten
  < 25% of the files present after migration. Otherwise MEDIUM.
- **Rewritten share** = files removed by OPTIMIZE since the migration (from history; PO compactions show up
  there too) ÷ files right after the clone. Falls back to PO system-table totals if the clone has aged out of history.
- **Causes**, first match wins:

| Cause | Evidence | Fix in remediation file |
|---|---|---|
| `shallow_clone` | CLONE with `isShallow=true` | `OPTIMIZE` (keep the Hive source until done) |
| `clustering_keys_changed` | `CLUSTER BY` with non-empty `oldClusteringColumns`, no `OPTIMIZE FULL` since | `OPTIMIZE ... FULL` |
| `over_partitioned` | ≥ 1,000 partitions and < 50 files per partition | `OPTIMIZE`, then consider recreating with liquid clustering |
| `scoped_optimize` | OPTIMIZE runs carry a `WHERE` predicate | `OPTIMIZE` with no predicate |
| `po_disabled` | `DESCRIBE TABLE EXTENDED` shows PO disabled | `OPTIMIZE`; enable PO on catalog/schema |
| `po_not_running` | PO enabled, no compactions recorded for the table | `OPTIMIZE`; check PO is active |
| `po_backlog` | PO has compacted the table, backlog remains | `OPTIMIZE` once; PO keeps it tidy afterwards |
| `no_maintenance` | No OPTIMIZE in history, PO status unknown | `OPTIMIZE` |
| `unknown` | Not enough metadata | `OPTIMIZE` |

  `po_backlog` is inferred: how PO picks what to compact isn't documented, so say "likely" when explaining it.
- **Explaining it:** a deep clone copies files as they are, so a fragmented Hive table becomes a fragmented UC
  table. Ongoing maintenance mostly handles new data, so the migrated files stay as they were. A one-time full
  OPTIMIZE clears them; PO then runs VACUUM to delete the old files after the retention period (7 days by default).
- **Verified on a live workspace (DBR 15.4, 2026-10):** adding liquid clustering to a deep-cloned table and running
  a plain OPTIMIZE rewrote all 3,000 cloned files into 1, so `OPTIMIZE FULL` is *not* needed after first enabling
  clustering. It's only needed when clustering keys change on already-clustered data. The `operationParameters` key
  marking `OPTIMIZE FULL` is still unconfirmed (matched loosely: any key containing "full" set to true).

### UC-SMALL-FILES: small-file problem (not migrated)
- **Flags:** Delta table ≥ 1 GB with ≥ 10,000 files averaging < 32 MB.
- **Severity:** HIGH at ≥ 100,000 files, otherwise MEDIUM. Same causes and fixes as MIG-BACKLOG.

### UC-HMS-REMAINING: tables left in hive_metastore
- **Flags:** any table in `hive_metastore`. Each is marked as having a known UC copy (from clone history,
  `upgraded_from`, or lineage) or not.
- **Severity:** MEDIUM if any have no known UC copy, otherwise LOW (migrated, but the Hive copies weren't retired).

### UC-NO-MAINTENANCE: large tables without maintenance
- **Flags:** ≥ 1,000 files, predictive optimization not enabled, and not both OPTIMIZE and VACUUM within the
  activity window (default 30 days).
- **Severity:** MEDIUM.

### UC-NO-LAYOUT: large tables with no data layout
- **Flags:** ≥ 100 GB, no partitioning and no liquid clustering. **Severity:** LOW.
- Remediation (`CLUSTER BY AUTO`) is marked REVIEW FIRST: it changes the layout and needs PO.

### UC-NON-DELTA: tables not stored as Delta
- **Flags:** MANAGED/EXTERNAL tables in Parquet, CSV, JSON, Avro, ORC, text. **Severity:** MEDIUM if ≥ 20, else LOW.

### UC-OWNER-USER: objects owned by individuals
- **Flags:** catalogs, schemas, tables whose owner looks like a user (contains `@`).
- **Severity:** MEDIUM if any catalog, or ≥ 10 schemas, is user-owned; else LOW.

### UC-NO-COMMENT: undocumented tables
- **Flags:** ≥ 50% of non-view tables have no comment. **Severity:** LOW.

### UC-EMPTY-SCHEMA: empty schemas
- **Flags:** schemas (other than `default`) with no tables or volumes. **Severity:** LOW.

## Jobs & pipelines (`jobs`)

Workspace-wide (the catalog filter doesn't apply). Runs come from `system.lakeflow.job_run_timeline` (falls back
to the Jobs API); job settings from the Jobs API; cost from `system.billing.usage` × `list_prices` (list price,
not the customer's discounted price: say so when quoting it). All over the activity window.

| Check | Flags | Severity |
|---|---|---|
| JOB-ALWAYS-FAILING | ≥ 2 completed runs, none succeeded | HIGH |
| JOB-FAILURE-RATE | ≥ 3 completed runs, some successes, ≥ 20% failed | MEDIUM |
| JOB-ALL-PURPOSE | Tasks use an existing (all-purpose) cluster, or the job billed all-purpose DBUs | MEDIUM |
| PIPE-FAILING | Pipeline's latest update in the window ended FAILED | MEDIUM |
| JOB-DURATION-GROWTH | Median successful run time in the 2nd half of the window ≥ 1.5× the 1st half, and ≥ 10 min | LOW |
| JOB-SCHEDULED-NOT-RUNNING | Unpaused schedule, no runs in the window (quarterly schedules show up too) | LOW |
| JOB-NO-FAILURE-ALERT | Active schedule/trigger, no on-failure email or webhook at job or task level | LOW |
| JOB-NO-TIMEOUT | Active schedule/trigger, no job or task timeout | LOW |
| JOB-RUN-AS-USER | Active schedule/trigger, runs as an individual user rather than a service principal | LOW |

Failure states counted: `ERROR`, `FAILED`, `TIMEDOUT`, `INTERNAL_ERROR` (live data uses `ERROR`). Cancelled runs
are excluded from failure rates. Durations are wall-clock (`end - start`): `run_duration_seconds` is often 0 on
the timeline table.

## Compute & warehouses (`compute`)

Workspace-wide. "All-purpose" = `cluster_source` `UI` or `API` (job and pipeline clusters are excluded from the
configuration checks). Cluster config from `system.compute.clusters` (latest row per cluster, falls back to the
Clusters API); utilization from `system.compute.node_timeline`; warehouses from the SQL Warehouses API.

| Check | Flags | Severity |
|---|---|---|
| CMP-AUTOTERM | Auto-termination off (HIGH) or > 60 min (MEDIUM) | HIGH / MEDIUM |
| CMP-RUNTIME | Runtime no longer offered by the workspace (HIGH) or non-LTS (LOW); clusters and job definitions | HIGH / LOW |
| CMP-ACCESS-MODE | Access mode `NONE`, `LEGACY_*`, or not set | MEDIUM |
| CMP-ALWAYS-ON | Driver up ≥ 80% of the window | MEDIUM |
| CMP-LOW-UTIL | ≥ 10 node-hours and average CPU < 20% (MEDIUM if ≥ 100 node-hours) | LOW / MEDIUM |
| CMP-NO-POLICY | No cluster policy | LOW |
| CMP-NO-TAGS | No custom tags | LOW |
| WH-AUTOSTOP | Auto-stop off (HIGH), or > 30 min serverless / > 60 min pro-classic (MEDIUM) | HIGH / MEDIUM |
| WH-NOT-SERVERLESS | Pro or classic warehouse | LOW |

"No longer offered" means the runtime isn't in this workspace's `clusters/spark-versions` list. LTS = the list
entry's name contains "LTS".

## Workspace settings & security (`settings`)

Read with GET calls only; most need a workspace admin. Unreadable settings are shown as ❔ unknown in the settings
table and never raise findings. Settings that depend on the customer's own policies (compliance security profile,
enhanced security monitoring, restrict workspace admins) are shown as ℹ️ info, not findings.

| Check | Flags | Severity |
|---|---|---|
| SET-IP-ACCESS | IP access lists not enabled (not needed if front-end Private Link restricts access) | MEDIUM |
| SET-PAT | Max token lifetime unset or > 90 days, or tokens that never expire / live > 90 days / unused 90 days | MEDIUM |
| SET-LEGACY | 'Disable legacy access' or 'Disable legacy DBFS' still off | MEDIUM |
| SET-ADMINS | admins group has > 10 members | LOW |
| SET-AUTO-UPDATE | Automatic cluster update off | LOW |
| SET-GLOBAL-INIT | Any enabled global init script | LOW |
| SET-AUDIT | Verbose audit logs off | LOW |
| SET-DEFAULT-CATALOG | Default catalog is `hive_metastore` | LOW |

## Not assessed yet

Cost (spend trends, SKU breakdown) and governance (grants) are shown as "Not assessed in this version".
Per-job and per-cluster list-price cost already appears in the jobs and compute evidence.
