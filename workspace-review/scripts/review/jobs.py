"""Jobs & pipelines: run reliability, compute choices, cost, and job configuration hygiene."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from .checks import CheckSpec
from .context import Finding, ReviewContext, evidence_records, plural, principal_kind

AREA = "jobs"
FAILED_STATES = {"ERROR", "FAILED", "TIMEDOUT", "TIMED_OUT", "INTERNAL_ERROR"}
SUCCEEDED_STATES = {"SUCCEEDED", "SUCCESS"}
# Jobs API result_state -> system table result_state
API_RESULT_STATES = {"SUCCESS": "SUCCEEDED", "FAILED": "FAILED", "TIMEDOUT": "TIMEDOUT", "CANCELED": "CANCELLED",
                     "INTERNAL_ERROR": "ERROR"}


def run(ctx: ReviewContext) -> None:
    if ctx.workspace_id is None:
        ctx.note("Jobs section skipped: workspace ID unknown, so system tables can't be filtered to this workspace.")
        return
    collect(ctx)
    evaluate(ctx)


# --- collection --------------------------------------------------------------------

def collect(ctx: ReviewContext) -> None:
    q = {n: ctx.render(sql) for n, sql in ctx.queries("jobs.sql").items()}

    if ctx.sources.get("lakeflow"):
        for name in ("jobs_current", "job_runs", "job_task_compute", "pipeline_updates"):
            try:
                ctx.add_stat(AREA, name, ctx.query_pdf(q[name]))
            except Exception as exc:
                ctx.record_error(f"{ctx.system_table('lakeflow')} ({name})", "jobs", exc)
    else:
        ctx.note(f"Job run history read from the Jobs API instead of {ctx.system_table('lakeflow')} (slower; pipelines not covered).")
        try:
            ctx.add_stat(AREA, "job_runs", _runs_from_api(ctx))
        except Exception as exc:
            ctx.record_error("Jobs API runs/list", "jobs", exc)

    if ctx.sources.get("billing") and ctx.sources.get("list_prices"):
        try:
            ctx.add_stat(AREA, "job_cost", ctx.query_pdf(q["job_cost"]))
        except Exception as exc:
            ctx.record_error(f"{ctx.system_table('billing')} (job cost)", "jobs", exc)

    try:
        jobs = ctx.api_list("/api/2.1/jobs/list", "jobs", {"expand_tasks": "true", "limit": 100})
        ctx.add_stat(AREA, "job_config", pd.DataFrame([parse_job_config(j) for j in jobs], columns=CONFIG_COLUMNS))
    except Exception as exc:
        ctx.record_error("Jobs API jobs/list", "jobs", exc)
        ctx.note("Job settings (schedules, alerts, timeouts, run-as) not readable; those checks were skipped.")


def _runs_from_api(ctx: ReviewContext) -> pd.DataFrame:
    since = int((datetime.now(timezone.utc) - timedelta(days=ctx.config["activity_window_days"])).timestamp() * 1000)
    runs = ctx.api_list("/api/2.1/jobs/runs/list", "runs",
                        {"start_time_from": since, "run_type": "JOB_RUN", "limit": 25})
    rows = []
    for r in runs:
        state = r.get("state") or {}
        rows.append({
            "job_id": str(r.get("job_id")), "run_id": str(r.get("run_id")),
            "start_time": pd.to_datetime(r.get("start_time"), unit="ms", utc=True) if r.get("start_time") else None,
            "end_time": pd.to_datetime(r.get("end_time"), unit="ms", utc=True) if r.get("end_time") else None,
            "result_state": API_RESULT_STATES.get(state.get("result_state"), state.get("result_state")),
            "termination_code": (r.get("status") or {}).get("termination_details", {}).get("code"),
            "trigger_type": r.get("trigger"),
        })
    return pd.DataFrame(rows)


CONFIG_COLUMNS = ["job_id", "name", "trigger_kind", "active_trigger", "has_timeout", "has_failure_alert",
                  "max_retries", "run_as", "existing_cluster_ids", "spark_versions", "task_count"]


def parse_job_config(job: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one jobs/list entry (with expand_tasks) into the fields the checks need."""
    s = job.get("settings") or {}
    tasks = s.get("tasks") or []
    triggers = {"schedule": s.get("schedule"), "file_arrival/table": s.get("trigger"), "continuous": s.get("continuous")}
    kind = next((k for k, v in triggers.items() if v), "manual")
    active = any(v and (v.get("pause_status") or "UNPAUSED") != "PAUSED" for v in triggers.values())

    def on_failure(holder: Dict[str, Any]) -> bool:
        return any((holder.get(k) or {}).get("on_failure") for k in ("email_notifications", "webhook_notifications"))

    clusters = [t.get("new_cluster") for t in tasks] + [jc.get("new_cluster") for jc in s.get("job_clusters") or []]
    run_as = s.get("run_as") or {}
    return {
        "job_id": str(job.get("job_id")),
        "name": s.get("name"),
        "trigger_kind": kind,
        "active_trigger": bool(active),
        "has_timeout": bool(s.get("timeout_seconds")) or any(t.get("timeout_seconds") for t in tasks),
        "has_failure_alert": on_failure(s) or any(on_failure(t) for t in tasks),
        "max_retries": max([t.get("max_retries") or 0 for t in tasks] or [0], key=lambda r: 10 ** 9 if r == -1 else r),
        "run_as": run_as.get("user_name") or run_as.get("service_principal_name")
                  or job.get("run_as_user_name") or job.get("creator_user_name"),
        "existing_cluster_ids": sorted({t["existing_cluster_id"] for t in tasks if t.get("existing_cluster_id")}),
        "spark_versions": sorted({c["spark_version"] for c in clusters if c and c.get("spark_version")}),
        "task_count": len(tasks),
    }


# --- pure logic (unit tested) -----------------------------------------------------------

def run_stats(runs: pd.DataFrame, window_days: int, now: Optional[datetime] = None) -> pd.DataFrame:
    """Per job: run counts by outcome, failure rate, latest failure/success, and median duration per window half."""
    cols = ["job_id", "runs", "succeeded", "failed", "completed", "fail_rate", "last_failure_code",
            "last_failure_time", "last_success_time", "median_min_first_half", "median_min_second_half"]
    if runs is None or runs.empty:
        return pd.DataFrame(columns=cols)
    now = pd.Timestamp(now or datetime.now(timezone.utc))
    midpoint = now - pd.Timedelta(days=window_days / 2)
    r = runs.copy()
    r["job_id"] = r["job_id"].astype(str)
    r["start_time"] = pd.to_datetime(r["start_time"], utc=True)
    r["end_time"] = pd.to_datetime(r["end_time"], utc=True)
    r["state"] = r["result_state"].fillna("").str.upper()
    r["failed"] = r["state"].isin(FAILED_STATES)
    r["succeeded"] = r["state"].isin(SUCCEEDED_STATES)
    # Wall-clock duration: run_duration_seconds is often 0 on the timeline table.
    r["minutes"] = (r["end_time"] - r["start_time"]).dt.total_seconds() / 60

    out = []
    for job_id, g in r.groupby("job_id"):
        fails, oks = g[g["failed"]], g[g["succeeded"]]
        done = len(fails) + len(oks)
        last_fail = fails.sort_values("start_time").iloc[-1] if len(fails) else None
        first, second = oks[oks["start_time"] < midpoint], oks[oks["start_time"] >= midpoint]
        out.append({
            "job_id": job_id, "runs": len(g), "succeeded": len(oks), "failed": len(fails), "completed": done,
            "fail_rate": round(len(fails) / done, 3) if done else None,
            "last_failure_code": None if last_fail is None else last_fail.get("termination_code"),
            "last_failure_time": None if last_fail is None else last_fail["start_time"],
            "last_success_time": oks["start_time"].max() if len(oks) else None,
            "median_min_first_half": round(first["minutes"].median(), 1) if len(first) else None,
            "median_min_second_half": round(second["minutes"].median(), 1) if len(second) else None,
            "_n_first": len(first), "_n_second": len(second),
        })
    return pd.DataFrame(out)


def evaluate(ctx: ReviewContext) -> None:
    prepare(ctx)
    for check in CHECKS:
        check.evaluate(ctx)


def prepare(ctx: ReviewContext) -> None:
    cfg, st = ctx.config, ctx.stats.get(AREA, {})
    names = _job_names(st)
    stats = run_stats(st.get("job_runs"), cfg["activity_window_days"])
    stats["name"] = stats["job_id"].map(names)
    ctx.add_stat(AREA, "run_stats", stats)
    config = st.get("job_config")
    cost = st.get("job_cost")
    cost = cost.assign(job_id=cost["job_id"].astype(str)) if cost is not None and not cost.empty else None

    _overview_stats(ctx, st, stats, config, cost)


def _prepared(ctx):
    st = ctx.stats.get(AREA, {})
    stats = st.get("run_stats", pd.DataFrame())
    config = st.get("job_config")
    cost = st.get("job_cost")
    cost = cost.assign(job_id=cost["job_id"].astype(str)) if cost is not None and not cost.empty else None
    return st, stats, config, cost


def _run_failures(ctx, check_id):
    _, stats, _, _ = _prepared(ctx)
    _check_failures(ctx, stats, ctx.config, check_id)


def _run_duration(ctx):
    _, stats, _, _ = _prepared(ctx)
    _check_duration_growth(ctx, stats, ctx.config)


def _run_all_purpose(ctx):
    st, stats, config, cost = _prepared(ctx)
    _check_all_purpose(ctx, config, cost, _job_names(st), set(stats["job_id"]) if not stats.empty else set())


def _run_config(ctx, check_id):
    _, stats, config, _ = _prepared(ctx)
    if config is not None and not config.empty:
        _check_config(ctx, config, stats, check_id)


def _run_pipelines(ctx):
    _check_pipelines(ctx, ctx.stats.get(AREA, {}).get("pipeline_updates"))


def _job_names(st: Dict[str, Any]) -> Dict[str, str]:
    names: Dict[str, str] = {}
    for key in ("job_config", "jobs_current"):
        df = st.get(key)
        if df is not None and not df.empty:
            for job_id, name in zip(df["job_id"].astype(str), df["name"]):
                names.setdefault(job_id, name)
    return names


def _overview_stats(ctx, st, stats, config, cost) -> None:
    runs = st.get("job_runs")
    total_runs = 0 if runs is None else len(runs)
    completed = int(stats["completed"].sum()) if not stats.empty else 0
    jobs_current = st.get("jobs_current")
    summary = {
        "jobs": len(config) if config is not None else (len(jobs_current) if jobs_current is not None else None),
        "jobs_with_active_trigger": int(config["active_trigger"].sum()) if config is not None else None,
        "jobs_that_ran": len(stats),
        "runs": total_runs,
        "success_rate_pct": round(100 * stats["succeeded"].sum() / completed, 1) if completed else None,
    }
    if cost is not None:
        summary["job_list_cost"] = round(float(cost["list_cost"].sum()), 2)
        summary["currency"] = cost["currency"].dropna().iloc[0] if cost["currency"].notna().any() else None
        top = cost.sort_values("list_cost", ascending=False).head(10).copy()
        top.insert(1, "name", top["job_id"].map(_job_names(st)))
        ctx.add_stat(AREA, "top_cost_jobs", top[["job_id", "name", "dbus", "list_cost", "all_purpose_list_cost"]])
    ctx.add_stat(AREA, "summary", summary)


def _check_failures(ctx, stats, cfg, only=None) -> None:
    if stats.empty:
        return
    always = stats[(stats["completed"] >= 2) & (stats["succeeded"] == 0)]
    flaky = stats[(stats["completed"] >= cfg["job_min_runs_for_failure_rate"]) & (stats["succeeded"] > 0)
                  & (stats["fail_rate"] >= cfg["job_failure_rate_medium"])]
    cols = ["job_id", "name", "completed", "failed", "fail_rate", "last_failure_code", "last_failure_time",
            "last_success_time"]
    days = cfg["activity_window_days"]
    if not always.empty and only in (None, "JOB-ALWAYS-FAILING"):
        ctx.add_finding(Finding(
            check_id="JOB-ALWAYS-FAILING", area=AREA, severity="HIGH",
            title=f"{plural(len(always), 'job')} failed every run in the last {days} days",
            summary="Jobs with at least 2 completed runs in the window and no successful run.",
            impact="Whatever these jobs produce is stale or missing, and every failed run still costs compute.",
            recommendation="Fix or pause each job. If it's no longer needed, delete it to stop the failed runs.",
            evidence=evidence_records(always.sort_values("failed", ascending=False)[cols]),
        ))
    if not flaky.empty and only in (None, "JOB-FAILURE-RATE"):
        ctx.add_finding(Finding(
            check_id="JOB-FAILURE-RATE", area=AREA, severity="MEDIUM",
            title=f"{plural(len(flaky), 'job')} failing in at least "
                  f"{cfg['job_failure_rate_medium']:.0%} of runs",
            summary=f"Jobs with {cfg['job_min_runs_for_failure_rate']}+ completed runs in the last {days} days and a "
                    "high failure rate, but some successes (intermittent failures).",
            impact="Unreliable pipelines delay downstream data and waste compute on failed attempts.",
            recommendation="Look at the most common failure codes. Add retries for transient errors and fix the "
                           "rest at the source.",
            evidence=evidence_records(flaky.sort_values("fail_rate", ascending=False)[cols]),
        ))


def _check_duration_growth(ctx, stats, cfg) -> None:
    if stats.empty:
        return
    s = stats[(stats["_n_first"] >= cfg["job_duration_min_runs_per_half"])
              & (stats["_n_second"] >= cfg["job_duration_min_runs_per_half"])].copy()
    if s.empty:
        return
    s["growth"] = (s["median_min_second_half"] / s["median_min_first_half"].where(s["median_min_first_half"] > 0)).round(2)
    grown = s[(s["growth"] >= cfg["job_duration_growth_ratio"])
              & (s["median_min_second_half"] >= cfg["job_duration_growth_min_minutes"])]
    if grown.empty:
        return
    ctx.add_finding(Finding(
        check_id="JOB-DURATION-GROWTH", area=AREA, severity="LOW",
        title=f"{plural(len(grown), 'job')} taking at least {cfg['job_duration_growth_ratio']}x longer than earlier "
              "in the window",
        summary="Median duration of successful runs in the second half of the activity window compared with the "
                "first half.",
        impact="Growing run times usually mean growing data without matching tuning, and lead to missed SLAs.",
        recommendation="Check the input data volume and the query profile for the slowest tasks; look for small "
                       "files, skew, or missing clustering on the tables they read.",
        evidence=evidence_records(grown.sort_values("growth", ascending=False)[
            ["job_id", "name", "median_min_first_half", "median_min_second_half", "growth"]]),
    ))


def _check_all_purpose(ctx, config, cost, names, ran: set) -> None:
    rows: Dict[str, Dict[str, Any]] = {}
    dormant = 0
    if config is not None and not config.empty:
        for r in config[config["existing_cluster_ids"].map(bool)].to_dict("records"):
            if not (r["active_trigger"] or r["job_id"] in ran):
                dormant += 1  # never triggered and didn't run: counted, not listed
                continue
            rows[r["job_id"]] = {"job_id": r["job_id"], "name": r["name"],
                                 "clusters": ", ".join(r["existing_cluster_ids"]), "active_trigger": r["active_trigger"],
                                 "ran_in_window": r["job_id"] in ran}
    if cost is not None:
        for r in cost[cost["all_purpose_dbus"] > 0].to_dict("records"):
            row = rows.setdefault(r["job_id"], {"job_id": r["job_id"], "name": names.get(r["job_id"]),
                                                "clusters": None, "active_trigger": None, "ran_in_window": True})
            row.update({"all_purpose_dbus": round(r["all_purpose_dbus"], 1),
                        "all_purpose_list_cost": round(r["all_purpose_list_cost"] or 0, 2)})
    ctx.add_stat(AREA, "dormant_jobs_on_all_purpose", dormant)
    if not rows:
        return
    ev = pd.DataFrame(rows.values())
    if "all_purpose_list_cost" in ev:
        ev = ev.sort_values("all_purpose_list_cost", ascending=False, na_position="last")
        spend = ev["all_purpose_list_cost"].fillna(0).sum()
        spend_text = f" They spent about {spend:,.0f} (list price) on all-purpose compute in the window."
    else:
        spend_text = ""
    ctx.add_finding(Finding(
        check_id="JOB-ALL-PURPOSE", area=AREA, severity="MEDIUM",
        title=f"{plural(len(ev), 'job')} running on all-purpose clusters",
        summary="Jobs that ran in the window or have an active schedule/trigger, and whose tasks use an existing "
                "(all-purpose) cluster or billed all-purpose DBUs." + spend_text
                + (f" A further {plural(dormant, 'dormant job')} (no trigger, no runs) also point at all-purpose "
                   "clusters and aren't listed." if dormant else ""),
        impact="All-purpose compute costs more per DBU than jobs compute, and shared clusters make jobs compete with "
               "interactive users.",
        recommendation="Move each job to serverless jobs compute or a job cluster.",
        evidence=evidence_records(ev),
        doc_url="https://docs.databricks.com/jobs/compute.html",
    ))


def _check_config(ctx, config, stats, only=None) -> None:
    active = config[config["active_trigger"]]
    if active.empty:
        return
    ran = set(stats["job_id"]) if not stats.empty else set()
    cols = ["job_id", "name", "trigger_kind", "run_as"]

    not_running = active[~active["job_id"].isin(ran) & (active["trigger_kind"] == "schedule")]
    if not not_running.empty and only in (None, "JOB-SCHEDULED-NOT-RUNNING"):
        ctx.add_finding(Finding(
            check_id="JOB-SCHEDULED-NOT-RUNNING", area=AREA, severity="LOW",
            title=f"{plural(len(not_running), 'scheduled job')} with no runs in the last "
                  f"{ctx.config['activity_window_days']} days",
            summary="Jobs with an active (unpaused) schedule that didn't run in the window. Schedules less frequent "
                    "than the window (e.g. quarterly) show up here too.",
            impact="A job that should be running isn't, or an old schedule is left behind.",
            recommendation="Confirm the schedule is still wanted; pause or delete it if not.",
            evidence=evidence_records(not_running[cols]),
        ))
    for check_id, mask, title, impact, rec in [
        ("JOB-NO-FAILURE-ALERT", ~active["has_failure_alert"], "with no failure notifications",
         "Failures go unnoticed until someone finds stale data.",
         "Add an email or webhook (e.g. Slack, PagerDuty) notification on failure to each job."),
        ("JOB-NO-TIMEOUT", ~active["has_timeout"], "with no timeout",
         "A hung run keeps its compute (and cost) until someone cancels it.",
         "Set a job or task timeout of a few times the normal run time."),
        ("JOB-RUN-AS-USER", active["run_as"].map(principal_kind) == "user", "running as an individual user",
         "The job breaks when that person leaves or loses access, and it runs with their personal permissions.",
         "Run scheduled jobs as a service principal."),
    ]:
        if only not in (None, check_id):
            continue
        hits = active[mask]
        if hits.empty:
            continue
        ctx.add_finding(Finding(
            check_id=check_id, area=AREA, severity="LOW",
            title=f"{plural(len(hits), 'scheduled or triggered job')} {title}",
            summary="Jobs with an active schedule, file-arrival/table trigger, or continuous mode.",
            impact=impact, recommendation=rec, evidence=evidence_records(hits[cols]),
        ))


def _check_pipelines(ctx, pipelines) -> None:
    if pipelines is None or pipelines.empty:
        return
    ctx.add_stat(AREA, "pipelines_summary", {"pipelines_updated": len(pipelines),
                                             "updates": int(pipelines["updates"].sum()),
                                             "failed_updates": int(pipelines["failed_updates"].sum())})
    failing = pipelines[pipelines["last_result_state"].fillna("").str.upper() == "FAILED"]
    if failing.empty:
        return
    ctx.add_finding(Finding(
        check_id="PIPE-FAILING", area=AREA, severity="MEDIUM",
        title=f"{plural(len(failing), 'pipeline')} whose latest update failed",
        summary="Lakeflow Declarative Pipelines whose most recent update in the window ended in FAILED.",
        impact="Tables maintained by these pipelines are not being refreshed.",
        recommendation="Open the pipeline's event log for the failing update and fix the error.",
        evidence=evidence_records(failing[["pipeline_id", "name", "pipeline_type", "updates", "failed_updates",
                                   "last_update_time", "run_as"]]),
    ))


CHECKS = (
    CheckSpec("JOB-ALWAYS-FAILING", AREA, "jobs", lambda ctx: _run_failures(ctx, "JOB-ALWAYS-FAILING"),
              ("activity_window_days",)),
    CheckSpec("JOB-FAILURE-RATE", AREA, "jobs", lambda ctx: _run_failures(ctx, "JOB-FAILURE-RATE"),
              ("job_min_runs_for_failure_rate", "job_failure_rate_medium", "activity_window_days")),
    CheckSpec("JOB-DURATION-GROWTH", AREA, "jobs", _run_duration,
              ("job_duration_min_runs_per_half", "job_duration_growth_ratio",
               "job_duration_growth_min_minutes")),
    CheckSpec("JOB-ALL-PURPOSE", AREA, "jobs", _run_all_purpose),
    CheckSpec("JOB-SCHEDULED-NOT-RUNNING", AREA, "jobs",
              lambda ctx: _run_config(ctx, "JOB-SCHEDULED-NOT-RUNNING"), ("activity_window_days",)),
    CheckSpec("JOB-NO-FAILURE-ALERT", AREA, "jobs", lambda ctx: _run_config(ctx, "JOB-NO-FAILURE-ALERT")),
    CheckSpec("JOB-NO-TIMEOUT", AREA, "jobs", lambda ctx: _run_config(ctx, "JOB-NO-TIMEOUT")),
    CheckSpec("JOB-RUN-AS-USER", AREA, "jobs", lambda ctx: _run_config(ctx, "JOB-RUN-AS-USER")),
    CheckSpec("PIPE-FAILING", AREA, "jobs", _run_pipelines),
)
