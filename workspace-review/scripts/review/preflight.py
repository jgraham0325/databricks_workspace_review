"""Step 0: identify the workspace and find out which data sources are readable."""

from __future__ import annotations

from .context import ReviewContext

# Shown in the report when a source is missing, so readers know what wasn't covered.
_MISSING_IMPACT = {
    "information_schema": "Unity Catalog inventory unavailable; the review cannot list tables.",
    "billing": "Cost analysis skipped.",
    "list_prices": "Costs shown in DBUs only (no list-price $ estimates).",
    "compute": "Cluster inventory falls back to the Clusters API (current clusters only, no history).",
    "node_timeline": "Cluster utilization (CPU/memory) not measured.",
    "warehouses": "Warehouse inventory falls back to the SQL Warehouses API.",
    "lakeflow": "Job history falls back to the Jobs API (slower, shorter history).",
    "table_lineage": "Migration detection cannot use lineage; relies on table history and properties only.",
    "predictive_optimization": "Predictive optimization activity unknown; migration-backlog causes are less certain.",
    "hive_metastore": "Legacy hive_metastore not inspected (disabled or not accessible).",
}


def run(ctx: ReviewContext) -> dict:
    spark = ctx.spark
    ctx.current_user = spark.sql("SELECT current_user() AS u").first()["u"]

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        ctx.workspace_id = int(w.get_workspace_id())
        ctx.workspace_url = w.config.host
    except Exception as exc:  # SDK missing or auth not available on this compute
        ctx.record_error("workspace", "preflight", exc)
        ctx.note("Could not determine the workspace ID; system-table results are not filtered to this workspace.")

    for name, probe in ctx.queries("preflight.sql").items():
        try:
            ctx.run_sql(probe).collect()
            ctx.sources[name] = True
        except Exception as exc:
            ctx.sources[name] = False
            ctx.record_error(name, "preflight", exc)
            ctx.note(f"`{name}` not readable: {_MISSING_IMPACT.get(name, 'section limited.')}")

    if ctx.sources.get("predictive_optimization"):
        # Record the metric keys PO actually emits on this workspace, so the migration check
        # can use them and the report can show them if parsing turns out to be wrong.
        try:
            keys = ctx.query_pdf(
                "SELECT DISTINCT operation_type, explode(map_keys(operation_metrics)) AS metric_key "
                "FROM system.storage.predictive_optimization_operations_history "
                "WHERE start_time >= current_timestamp() - INTERVAL 90 DAYS"
            )
            ctx.add_stat("preflight", "po_metric_keys", keys)
        except Exception as exc:
            ctx.record_error("predictive_optimization", "preflight metric keys", exc)

    summary = {
        "current_user": ctx.current_user,
        "workspace_id": ctx.workspace_id,
        "workspace_url": ctx.workspace_url,
        "activity_window_days": ctx.config["activity_window_days"],
        "include_catalogs": ctx.config["include_catalogs"] or "(all)",
        "excluded_catalogs": ctx.config["excluded_catalogs"] + ctx.config["extra_excluded_catalogs"],
        "sources": dict(ctx.sources),
    }
    ctx.add_stat("preflight", "summary", summary)
    return summary
