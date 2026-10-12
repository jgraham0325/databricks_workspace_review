"""Compute: all-purpose cluster configuration and utilization, runtime versions, and SQL warehouses."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import pandas as pd

from .checks import CheckSpec
from .context import Finding, ReviewContext, evidence_records, plural

AREA = "compute"
ALL_PURPOSE_SOURCES = {"UI", "API"}
CLUSTER_COLUMNS = ["cluster_id", "cluster_name", "owned_by", "cluster_source", "dbr_version", "data_security_mode",
                   "policy_id", "auto_termination_minutes", "worker_count", "min_autoscale_workers",
                   "max_autoscale_workers", "tags"]
WAREHOUSE_COLUMNS = ["warehouse_id", "warehouse_name", "warehouse_type", "is_serverless", "warehouse_size",
                     "max_clusters", "auto_stop_minutes", "state"]


def run(ctx: ReviewContext) -> None:
    if ctx.workspace_id is None:
        ctx.note("Compute section skipped: workspace ID unknown, so system tables can't be filtered to this workspace.")
        return
    collect(ctx)
    evaluate(ctx)


# --- collection --------------------------------------------------------------------

def collect(ctx: ReviewContext) -> None:
    q = {n: ctx.render(sql) for n, sql in ctx.queries("compute.sql").items()}

    clusters = None
    if ctx.sources.get("compute"):
        try:
            clusters = ctx.query_pdf(q["clusters_current"])
        except Exception as exc:
            ctx.record_error("system.compute.clusters", "compute", exc)
    if clusters is None:
        try:
            clusters = clusters_from_api(ctx.api_list("/api/2.1/clusters/list", "clusters", {"page_size": 100}))
            ctx.note("Cluster inventory read from the Clusters API (system.compute.clusters not readable).")
        except Exception as exc:
            ctx.record_error("Clusters API", "compute", exc)
    if clusters is not None:
        ctx.add_stat(AREA, "clusters", clusters)

    try:
        versions = ctx.api_get("/api/2.0/clusters/spark-versions").get("versions") or []
        ctx.add_stat(AREA, "spark_versions", pd.DataFrame(versions, columns=["key", "name"]))
    except Exception as exc:
        ctx.record_error("Clusters API spark-versions", "compute", exc)
        ctx.note("Available runtime versions unknown; the runtime version check was skipped.")

    if ctx.sources.get("node_timeline"):
        try:
            ctx.add_stat(AREA, "utilization", ctx.query_pdf(q["cluster_utilization"]))
        except Exception as exc:
            ctx.record_error("system.compute.node_timeline", "compute", exc)
    if ctx.sources.get("billing") and ctx.sources.get("list_prices"):
        try:
            ctx.add_stat(AREA, "cost", ctx.query_pdf(q["compute_cost"]))
        except Exception as exc:
            ctx.record_error("system.billing (compute cost)", "compute", exc)

    try:
        ctx.add_stat(AREA, "warehouses", warehouses_from_api(ctx.api_get("/api/2.0/sql/warehouses").get("warehouses") or []))
    except Exception as exc:
        ctx.record_error("SQL Warehouses API", "compute", exc)
        if ctx.sources.get("warehouses"):
            try:
                ctx.add_stat(AREA, "warehouses", warehouses_from_system(ctx.query_pdf(q["warehouses_current"])))
                ctx.note("Warehouse inventory read from system.compute.warehouses (API not readable).")
            except Exception as exc2:
                ctx.record_error("system.compute.warehouses", "compute", exc2)


def clusters_from_api(items: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for c in items:
        autoscale = c.get("autoscale") or {}
        rows.append({
            "cluster_id": c.get("cluster_id"), "cluster_name": c.get("cluster_name"),
            "owned_by": c.get("single_user_name") or c.get("creator_user_name"),
            "cluster_source": c.get("cluster_source"), "dbr_version": c.get("spark_version"),
            "data_security_mode": c.get("data_security_mode"), "policy_id": c.get("policy_id"),
            "auto_termination_minutes": c.get("autotermination_minutes"), "worker_count": c.get("num_workers"),
            "min_autoscale_workers": autoscale.get("min_workers"), "max_autoscale_workers": autoscale.get("max_workers"),
            "tags": c.get("custom_tags") or {},
        })
    return pd.DataFrame(rows, columns=CLUSTER_COLUMNS)


def warehouses_from_api(items: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = [{
        "warehouse_id": w.get("id"), "warehouse_name": w.get("name"),
        "warehouse_type": "SERVERLESS" if w.get("enable_serverless_compute") else (w.get("warehouse_type") or "CLASSIC"),
        "is_serverless": bool(w.get("enable_serverless_compute")), "warehouse_size": w.get("cluster_size"),
        "max_clusters": w.get("max_num_clusters"), "auto_stop_minutes": w.get("auto_stop_mins"),
        "state": w.get("state"),
    } for w in items]
    return pd.DataFrame(rows, columns=WAREHOUSE_COLUMNS)


def warehouses_from_system(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={"warehouse_size": "warehouse_size"}).copy()
    out["is_serverless"] = out["warehouse_type"].str.upper() == "SERVERLESS"
    out["max_clusters"] = out["max_clusters"]
    out["state"] = None
    return out[WAREHOUSE_COLUMNS]


# --- pure logic (unit tested) -----------------------------------------------------------

def classify_dbr(version: Optional[str], available: Set[str], lts: Set[str]) -> str:
    """'unsupported' (no longer offered), 'non_lts', 'ok', or 'unknown' (custom image / no version / no list)."""
    if not version or not available or version.startswith("custom:"):
        return "unknown"
    if version not in available:
        return "unsupported"
    return "ok" if version in lts else "non_lts"


def evaluate(ctx: ReviewContext) -> None:
    prepare(ctx)
    for check in CHECKS:
        check.evaluate(ctx)


def prepare(ctx: ReviewContext) -> None:
    cfg, st = ctx.config, ctx.stats.get(AREA, {})
    clusters = st.get("clusters")
    cost = _cost_map(st.get("cost"))
    util = st.get("utilization")

    if clusters is not None and not clusters.empty:
        ap = clusters[clusters["cluster_source"].isin(ALL_PURPOSE_SOURCES)].copy()
        ap["list_cost"] = ap["cluster_id"].map(cost)
        if util is not None and not util.empty:
            ap = ap.merge(util[["cluster_id", "node_hours", "driver_hours", "avg_cpu_pct", "avg_mem_pct"]],
                          on="cluster_id", how="left")
        ctx.add_stat(AREA, "all_purpose_clusters", ap)
        _cluster_overview(ctx, clusters, ap)

    warehouses = st.get("warehouses")
    if warehouses is not None and not warehouses.empty:
        wh = warehouses.copy()
        wh["list_cost"] = wh["warehouse_id"].map(cost)
        ctx.add_stat(AREA, "warehouses", wh)
    _cost_overview(ctx, st.get("cost"))


def _prepared(ctx):
    st = ctx.stats.get(AREA, {})
    return st, st.get("all_purpose_clusters", pd.DataFrame()), _cost_map(st.get("cost"))


def _run_ap(ctx, fn):
    _, ap, _ = _prepared(ctx)
    if not ap.empty:
        fn(ap)


def _run_low_util(ctx):
    st, _, cost = _prepared(ctx)
    util = st.get("utilization")
    if util is not None and not util.empty:
        _check_low_utilization(ctx, util, st.get("clusters"), cost, ctx.config)


def _run_runtime(ctx):
    st, ap, _ = _prepared(ctx)
    _check_dbr(ctx, ap, st.get("spark_versions"), ctx.stats.get("jobs", {}).get("job_config"),
               ctx.stats.get("jobs", {}).get("run_stats"))


def _run_warehouse(ctx, check_id):
    warehouses = ctx.stats.get(AREA, {}).get("warehouses")
    if warehouses is not None and not warehouses.empty:
        _check_warehouses(ctx, warehouses, ctx.config, check_id)


def _cost_map(cost: Optional[pd.DataFrame]) -> Dict[str, float]:
    if cost is None or cost.empty:
        return {}
    return {k: round(float(v or 0), 2) for k, v in zip(cost["compute_id"], cost["list_cost"])}


def _cluster_overview(ctx, clusters, ap) -> None:
    ctx.add_stat(AREA, "summary", {
        "clusters": len(clusters),
        "all_purpose_clusters": len(ap),
        "clusters_by_source": clusters["cluster_source"].fillna("(none)").value_counts().to_dict(),
    })
    if not ap.empty:
        ctx.add_stat(AREA, "all_purpose_by_runtime", ap["dbr_version"].fillna("(none)").value_counts().to_dict())


def _cost_overview(ctx, cost) -> None:
    if cost is None or cost.empty:
        return
    by_kind = cost.groupby("compute_kind")["list_cost"].sum().round(2).to_dict()
    ctx.stats[AREA].setdefault("summary", {}).update({f"{k}_list_cost": v for k, v in by_kind.items()})


def _cols(df, cols):
    return df[[c for c in cols if c in df.columns]]


def _check_autotermination(ctx, ap, cfg) -> None:
    limit = cfg["cluster_autoterm_max_minutes"]
    minutes = ap["auto_termination_minutes"]
    never = ap[minutes.isna() | (minutes == 0)]
    long = ap[minutes > limit]
    hits = pd.concat([never, long])
    if hits.empty:
        return
    ctx.add_finding(Finding(
        check_id="CMP-AUTOTERM", area=AREA, severity="HIGH" if not never.empty else "MEDIUM",
        title=f"{plural(len(hits), 'all-purpose cluster')} that never auto-terminate or wait over {limit} minutes",
        summary=f"{len(never)} with auto-termination off, {len(long)} with a timeout over {limit} minutes.",
        impact="Idle clusters keep billing. A cluster with auto-termination off runs until someone stops it.",
        recommendation=f"Set auto-termination to {min(limit, 30)}-{limit} minutes, ideally enforced with a cluster "
                       "policy.",
        evidence=evidence_records(_cols(hits, ["cluster_id", "cluster_name", "owned_by", "auto_termination_minutes",
                                       "list_cost"]).sort_values("auto_termination_minutes", na_position="first")),
    ))


def _check_access_mode(ctx, ap) -> None:
    mode = ap["data_security_mode"].fillna("").str.upper()
    legacy = ap[(mode == "") | (mode == "NONE") | mode.str.startswith("LEGACY")].copy()
    if legacy.empty:
        return
    legacy["data_security_mode"] = legacy["data_security_mode"].fillna("(not set: no isolation)")
    ctx.add_finding(Finding(
        check_id="CMP-ACCESS-MODE", area=AREA, severity="MEDIUM",
        title=f"{plural(len(legacy), 'all-purpose cluster')} without Unity Catalog access mode",
        summary="Clusters in No isolation shared, a legacy mode, or with no access mode set.",
        impact="These clusters can't read Unity Catalog data and bypass its access controls and lineage.",
        recommendation="Switch to Standard (shared) or Dedicated (single user) access mode, and use a cluster policy "
                       "to stop new no-isolation clusters.",
        evidence=evidence_records(_cols(legacy, ["cluster_id", "cluster_name", "owned_by", "data_security_mode", "dbr_version"])),
        doc_url="https://docs.databricks.com/compute/access-mode-limitations.html",
    ))


def _check_dbr(ctx, ap, versions, job_config, run_stats=None) -> None:
    if versions is None or versions.empty:
        return
    available = set(versions["key"])
    lts = set(versions.loc[versions["name"].fillna("").str.contains("LTS"), "key"])
    rows = [{"kind": "all-purpose cluster", "id": r["cluster_id"], "name": r["cluster_name"], "runtime": r["dbr_version"]}
            for r in ap.to_dict("records")]
    dormant = 0
    if job_config is not None and not job_config.empty:
        ran = set(run_stats["job_id"]) if run_stats is not None and not run_stats.empty else set()
        for r in job_config.to_dict("records"):
            if not r["spark_versions"]:
                continue
            if not (r["active_trigger"] or r["job_id"] in ran):
                dormant += 1  # never triggered and didn't run: counted, not listed
                continue
            rows += [{"kind": "job definition", "id": r["job_id"], "name": r["name"], "runtime": v}
                     for v in r["spark_versions"]]
    ctx.add_stat(AREA, "dormant_jobs_with_runtime", dormant)
    if not rows:
        return
    ev = pd.DataFrame(rows)
    ev["status"] = ev["runtime"].map(lambda v: classify_dbr(v, available, lts))
    ctx.add_stat(AREA, "runtime_status", ev["status"].value_counts().to_dict())
    bad = ev[ev["status"].isin(["unsupported", "non_lts"])].sort_values("status", ascending=False)
    if bad.empty:
        return
    unsupported = int((bad["status"] == "unsupported").sum())
    ctx.add_finding(Finding(
        check_id="CMP-RUNTIME", area=AREA, severity="HIGH" if unsupported else "LOW",
        title=f"{plural(len(bad), 'cluster or job', 'clusters and jobs')} on an unsupported or non-LTS "
              "Databricks Runtime",
        summary=f"{unsupported} use a runtime this workspace no longer offers; {len(bad) - unsupported} use a "
                "short-term (non-LTS) runtime. Covers all-purpose clusters and jobs that ran in the window or have "
                "an active schedule/trigger." + (f" {plural(dormant, 'dormant job')} not listed." if dormant else ""),
        impact="Unsupported runtimes get no security fixes and can stop starting without warning. Non-LTS runtimes "
               "go out of support after about six months.",
        recommendation="Move to the latest LTS runtime, starting with unsupported ones. Test jobs on the new "
                       "runtime first.",
        evidence=evidence_records(bad),
        doc_url="https://docs.databricks.com/release-notes/runtime/index.html",
    ))


def _check_always_on(ctx, ap, cfg) -> None:
    if "driver_hours" not in ap:
        return
    window_hours = cfg["activity_window_days"] * 24
    on = ap[ap["driver_hours"].fillna(0) >= cfg["cluster_always_on_share"] * window_hours].copy()
    if on.empty:
        return
    on["uptime_pct"] = (100 * on["driver_hours"] / window_hours).round(0)
    ctx.add_finding(Finding(
        check_id="CMP-ALWAYS-ON", area=AREA, severity="MEDIUM",
        title=f"{plural(len(on), 'all-purpose cluster')} running almost all the time",
        summary=f"Clusters up for at least {cfg['cluster_always_on_share']:.0%} of the last "
                f"{cfg['activity_window_days']} days.",
        impact="Round-the-clock all-purpose clusters are often the largest avoidable cost in a workspace.",
        recommendation="Check whether the work is scheduled. If so, move it to jobs compute; otherwise shorten "
                       "auto-termination so the cluster stops when idle.",
        evidence=evidence_records(_cols(on, ["cluster_id", "cluster_name", "owned_by", "uptime_pct", "avg_cpu_pct",
                                     "list_cost"]).sort_values("uptime_pct", ascending=False)),
    ))


def _check_low_utilization(ctx, util, clusters, cost, cfg) -> None:
    low = util[(util["node_hours"] >= cfg["cluster_low_util_min_node_hours"])
               & (util["avg_cpu_pct"] < cfg["cluster_low_cpu_pct"])].copy()
    if low.empty:
        return
    if clusters is not None and not clusters.empty:
        low = low.merge(clusters[["cluster_id", "cluster_name", "cluster_source", "owned_by"]], on="cluster_id", how="left")
    low["list_cost"] = low["cluster_id"].map(cost)
    low["node_hours"] = low["node_hours"].round(1)
    low["avg_cpu_pct"] = low["avg_cpu_pct"].round(1)
    low["avg_mem_pct"] = low["avg_mem_pct"].round(1)
    big = (low["node_hours"] >= cfg["cluster_low_util_medium_node_hours"]).any()
    ctx.add_finding(Finding(
        check_id="CMP-LOW-UTIL", area=AREA, severity="MEDIUM" if big else "LOW",
        title=f"{plural(len(low), 'cluster')} averaging under {cfg['cluster_low_cpu_pct']}% CPU",
        summary=f"Clusters with at least {cfg['cluster_low_util_min_node_hours']} node-hours in the window and low "
                "average CPU across all nodes.",
        impact="Paying for nodes that mostly sit idle.",
        recommendation="Use fewer or smaller workers, enable autoscaling, or move the workload to serverless.",
        evidence=evidence_records(_cols(low, ["cluster_id", "cluster_name", "cluster_source", "owned_by", "node_hours",
                                      "avg_cpu_pct", "avg_mem_pct", "list_cost"]).sort_values("node_hours", ascending=False)),
    ))


def _check_policy_and_tags(ctx, ap, only=None) -> None:
    no_policy = ap[ap["policy_id"].isna() | (ap["policy_id"].fillna("") == "")]
    if not no_policy.empty and only in (None, "CMP-NO-POLICY"):
        ctx.add_finding(Finding(
            check_id="CMP-NO-POLICY", area=AREA, severity="LOW",
            title=f"{plural(len(no_policy), 'all-purpose cluster')} not governed by a cluster policy",
            summary="Clusters created without a cluster policy.",
            impact="Without policies, nothing enforces auto-termination, size limits, access mode, or cost tags.",
            recommendation="Create cluster policies for common use cases and restrict unrestricted cluster creation.",
            evidence=evidence_records(_cols(no_policy, ["cluster_id", "cluster_name", "owned_by", "list_cost"])),
            doc_url="https://docs.databricks.com/admin/clusters/policies.html",
        ))
    untagged = ap[ap["tags"].map(lambda t: not t)]
    if not untagged.empty and only in (None, "CMP-NO-TAGS"):
        ctx.add_finding(Finding(
            check_id="CMP-NO-TAGS", area=AREA, severity="LOW",
            title=f"{plural(len(untagged), 'all-purpose cluster')} without custom tags",
            summary="Clusters with no custom tags (e.g. team, cost centre, project).",
            impact="Their cost can't be attributed to a team or project in billing reports.",
            recommendation="Require tags such as team and cost_center through cluster policies.",
            evidence=evidence_records(_cols(untagged, ["cluster_id", "cluster_name", "owned_by", "list_cost"])),
        ))


def _check_warehouses(ctx, wh, cfg, only=None) -> None:
    def limit(row):
        return cfg["warehouse_autostop_max_minutes_serverless" if row["is_serverless"]
                   else "warehouse_autostop_max_minutes_classic"]

    stop = wh["auto_stop_minutes"]
    never = wh[stop.isna() | (stop == 0)]
    long = wh[[(m or 0) > limit(r) for m, r in zip(stop, wh.to_dict("records"))]]
    hits = pd.concat([never, long])
    if not hits.empty and only in (None, "WH-AUTOSTOP"):
        ctx.add_finding(Finding(
            check_id="WH-AUTOSTOP", area=AREA, severity="HIGH" if not never.empty else "MEDIUM",
            title=f"{plural(len(hits), 'SQL warehouse')} that never stop or wait long before stopping",
            summary=f"{len(never)} with auto-stop off; {len(long)} above "
                    f"{cfg['warehouse_autostop_max_minutes_serverless']} minutes (serverless) or "
                    f"{cfg['warehouse_autostop_max_minutes_classic']} minutes (pro/classic).",
            impact="Warehouses bill while idle until they auto-stop.",
            recommendation="Serverless warehouses start in seconds, so 5-10 minutes is usually enough; pro and classic "
                           "warehouses take longer to start, so 30-45 minutes.",
            evidence=evidence_records(_cols(hits, ["warehouse_id", "warehouse_name", "warehouse_type", "warehouse_size",
                                           "auto_stop_minutes", "list_cost"])),
        ))
    classic = wh[~wh["is_serverless"]]
    if not classic.empty and only in (None, "WH-NOT-SERVERLESS"):
        ctx.add_finding(Finding(
            check_id="WH-NOT-SERVERLESS", area=AREA, severity="LOW",
            title=f"{plural(len(classic), 'SQL warehouse')} not using serverless",
            summary="Pro or classic warehouses.",
            impact="They start more slowly, so they're usually given long auto-stop times and sit idle for longer.",
            recommendation="Switch to serverless SQL warehouses where serverless is available in the region.",
            evidence=evidence_records(_cols(classic, ["warehouse_id", "warehouse_name", "warehouse_type", "warehouse_size",
                                              "auto_stop_minutes", "list_cost"])),
            doc_url="https://docs.databricks.com/compute/sql-warehouse/warehouse-types.html",
        ))

CHECKS = (
    CheckSpec("CMP-AUTOTERM", AREA, "compute",
              lambda ctx: _run_ap(ctx, lambda ap: _check_autotermination(ctx, ap, ctx.config)),
              ("cluster_autoterm_max_minutes",)),
    CheckSpec("CMP-ACCESS-MODE", AREA, "compute",
              lambda ctx: _run_ap(ctx, lambda ap: _check_access_mode(ctx, ap))),
    CheckSpec("CMP-RUNTIME", AREA, "compute", _run_runtime, depends_on=("jobs",)),
    CheckSpec("CMP-ALWAYS-ON", AREA, "compute",
              lambda ctx: _run_ap(ctx, lambda ap: _check_always_on(ctx, ap, ctx.config)),
              ("activity_window_days", "cluster_always_on_share")),
    CheckSpec("CMP-LOW-UTIL", AREA, "compute", _run_low_util,
              ("cluster_low_cpu_pct", "cluster_low_util_min_node_hours", "cluster_low_util_medium_node_hours")),
    CheckSpec("CMP-NO-POLICY", AREA, "compute",
              lambda ctx: _run_ap(ctx, lambda ap: _check_policy_and_tags(ctx, ap, "CMP-NO-POLICY"))),
    CheckSpec("CMP-NO-TAGS", AREA, "compute",
              lambda ctx: _run_ap(ctx, lambda ap: _check_policy_and_tags(ctx, ap, "CMP-NO-TAGS"))),
    CheckSpec("WH-AUTOSTOP", AREA, "compute", lambda ctx: _run_warehouse(ctx, "WH-AUTOSTOP"),
              ("warehouse_autostop_max_minutes_serverless", "warehouse_autostop_max_minutes_classic")),
    CheckSpec("WH-NOT-SERVERLESS", AREA, "compute", lambda ctx: _run_warehouse(ctx, "WH-NOT-SERVERLESS")),
)
