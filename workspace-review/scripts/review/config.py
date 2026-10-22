"""Default settings and thresholds for the workspace review.

Every value can be overridden at run time:
    ctx = ReviewContext.create(spark, skill_dir, overrides={"small_files_high_count": 50_000})
"""

# Implemented review areas. Empty include_areas means all of these.
REVIEW_AREAS = ("uc", "jobs", "compute", "settings")

DEFAULTS = {
    # --- Run scope -----------------------------------------------------------
    # How far back to look at *activity*: maintenance runs (e.g. "no OPTIMIZE in the last N days"),
    # and later job runs and cost. It does not filter which tables are reviewed; tables of any age are
    # included, and migration history is read in full regardless of this window.
    "activity_window_days": 30,
    # If non-empty, review only these catalogs. Include "hive_metastore" to also inventory legacy
    # Hive tables; with an empty list, hive_metastore is inventoried whenever it's readable.
    "include_catalogs": [],
    # If non-empty, review only these schemas (Unity Catalog data only). Each entry is a schema
    # name (any in-scope catalog) or catalog.schema. Empty = all schemas in in-scope catalogs.
    "include_schemas": [],
    # If non-empty, run only these areas: uc, jobs, compute, settings. Empty = all four.
    # ["uc"] is tables only (inventory, small files, migration); skips jobs, compute, and settings.
    "include_areas": [],
    # Catalog holding system tables (information_schema, billing, compute, lakeflow, …).
    # Some workspaces expose a views catalog with the same schema/table names, filtered to the
    # caller, instead of granting SELECT on `system`. Override to that catalog name.
    "system_catalog": "system",
    # Catalogs never reviewed (system/internal/sample data). Applied on top of include_catalogs.
    "excluded_catalogs": ["system", "samples", "__databricks_internal"],
    # Extra catalogs the caller wants skipped (e.g. Delta Sharing / foreign catalogs).
    "extra_excluded_catalogs": [],
    # Safety cap on per-table inspection (DESCRIBE DETAIL etc.).
    "max_tables_detail": 5000,
    # Parallel threads for per-table metadata commands.
    "detail_parallelism": 16,
    # Deep scan reads every file's size via _metadata (full table scan). Off by default.
    "deep_scan": False,
    "deep_scan_max_tables": 10,

    # --- Table detail ---------------------------------------------------------
    # Only fetch DESCRIBE HISTORY / PO status for tables with at least this many files.
    "history_min_files": 1000,
    # Rows of history to read per table (newest first). Large enough to reach the clone entry
    # on most tables while keeping the command cheap.
    "history_limit": 2000,

    # --- Small files ------------------------------------------------------------
    "small_file_avg_mb": 32,
    "small_files_min_table_gb": 1,
    "small_files_medium_count": 10_000,
    "small_files_high_count": 100_000,

    # --- Migration backlog ---------------------------------------------------
    # High severity if maintenance has rewritten less than this share of the cloned files.
    "migration_rewrite_ratio_high": 0.25,
    # A partitioned table counts as over-partitioned when it has at least this many partitions
    # and, on average, fewer than this many files per partition.
    "over_partitioned_min_partitions": 1000,
    "over_partitioned_max_files_per_partition": 50,

    # --- Other table checks ------------------------------------------------------
    "no_layout_min_table_gb": 100,
    "missing_comment_low_pct": 50,

    # --- Jobs & pipelines (all over the activity window) ---------------------------
    "job_min_runs_for_failure_rate": 3,
    "job_failure_rate_medium": 0.2,
    # Duration growth: median of the second half of the window vs the first half.
    "job_duration_growth_ratio": 1.5,
    "job_duration_growth_min_minutes": 10,
    "job_duration_min_runs_per_half": 3,

    # --- Compute -------------------------------------------------------------------
    "cluster_autoterm_max_minutes": 60,
    "cluster_low_cpu_pct": 20,
    "cluster_low_util_min_node_hours": 10,
    "cluster_low_util_medium_node_hours": 100,
    # Share of the window an all-purpose cluster's driver was up to count as "always on".
    "cluster_always_on_share": 0.8,
    "warehouse_autostop_max_minutes_serverless": 30,
    "warehouse_autostop_max_minutes_classic": 60,

    # --- Workspace settings ---------------------------------------------------------
    "token_max_lifetime_days": 90,
    "admins_max_members": 10,

    # --- Health score ------------------------------------------------------------
    "score_weights": {"HIGH": 20, "MEDIUM": 8, "LOW": 3},
    "score_bands": [(85, "Good"), (60, "Needs attention"), (0, "At risk")],
}
