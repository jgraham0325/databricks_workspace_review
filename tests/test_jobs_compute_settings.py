"""Jobs, compute, and settings checks on canned data shaped like real API and system-table output."""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_DIR = os.path.join(ROOT, "workspace-review")

from review import compute, jobs, settings  # noqa: E402
from review.context import ReviewContext, decimals_to_float  # noqa: E402
from review.config import DEFAULTS  # noqa: E402

NOW = datetime(2026, 10, 9, tzinfo=timezone.utc)


def _ctx():
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR)
    ctx.workspace_id = 1
    return ctx


def _run(job_id, days_ago, state, minutes=5, code=None):
    start = NOW - timedelta(days=days_ago)
    return {"job_id": job_id, "run_id": f"{job_id}-{days_ago}-{state}", "start_time": start,
            "end_time": start + timedelta(minutes=minutes), "result_state": state, "termination_code": code}


# --- jobs ---------------------------------------------------------------------------

def test_run_stats_failure_rates_and_halves():
    runs = pd.DataFrame(
        [_run("always", d, "ERROR", code="RUN_EXECUTION_ERROR") for d in (1, 5, 9)]
        + [_run("flaky", d, "SUCCEEDED") for d in (2, 4, 6)] + [_run("flaky", 8, "FAILED")]
        + [_run("slow", d, "SUCCEEDED", minutes=10) for d in (20, 22, 25)]
        + [_run("slow", d, "SUCCEEDED", minutes=30) for d in (1, 3, 5)]
        + [_run("running", 0, None)])
    st = jobs.run_stats(runs, 30, now=NOW).set_index("job_id")
    assert st.loc["always", ["completed", "failed", "succeeded"]].tolist() == [3, 3, 0]
    assert st.loc["always", "last_failure_code"] == "RUN_EXECUTION_ERROR"
    assert st.loc["flaky", "fail_rate"] == 0.25
    assert st.loc["slow", "median_min_first_half"] == 10 and st.loc["slow", "median_min_second_half"] == 30
    assert st.loc["running", "completed"] == 0 and pd.isna(st.loc["running", "fail_rate"])


def test_parse_job_config():
    job = {"job_id": 7, "creator_user_name": "a@x.com", "settings": {
        "name": "nightly", "schedule": {"quartz_cron_expression": "0 0 2 * * ?", "pause_status": "UNPAUSED"},
        "email_notifications": {}, "timeout_seconds": 0,
        "tasks": [{"task_key": "a", "existing_cluster_id": "0101-abc", "max_retries": 2},
                  {"task_key": "b", "new_cluster": {"spark_version": "13.3.x-scala2.12"}, "timeout_seconds": 600,
                   "webhook_notifications": {"on_failure": [{"id": "slack"}]}}],
        "run_as": {"service_principal_name": "11111111-2222-3333-4444-555555555555"}}}
    c = jobs.parse_job_config(job)
    assert c["job_id"] == "7" and c["trigger_kind"] == "schedule" and c["active_trigger"] is True
    assert c["has_timeout"] is True and c["has_failure_alert"] is True and c["max_retries"] == 2
    assert c["existing_cluster_ids"] == ["0101-abc"] and c["spark_versions"] == ["13.3.x-scala2.12"]
    assert c["run_as"] == "11111111-2222-3333-4444-555555555555"
    paused = jobs.parse_job_config({"job_id": 8, "settings": {"schedule": {"pause_status": "PAUSED"}, "tasks": []}})
    assert paused["active_trigger"] is False and paused["has_failure_alert"] is False
    manual = jobs.parse_job_config({"job_id": 9, "creator_user_name": "b@x.com", "settings": {"tasks": []}})
    assert manual["trigger_kind"] == "manual" and manual["run_as"] == "b@x.com"


def test_jobs_evaluate_findings():
    ctx = _ctx()
    runs = pd.DataFrame([_run("1", d, "ERROR") for d in (1, 2)] + [_run("2", d, "SUCCEEDED") for d in (1, 2, 3)])
    config = pd.DataFrame([
        jobs.parse_job_config({"job_id": 1, "creator_user_name": "a@x.com", "settings": {
            "name": "broken", "schedule": {"pause_status": "UNPAUSED"},
            "tasks": [{"task_key": "t", "existing_cluster_id": "c1"}]}}),
        jobs.parse_job_config({"job_id": 3, "settings": {"name": "stale", "schedule": {"pause_status": "UNPAUSED"},
                                                         "tasks": [], "run_as": {"user_name": "z@x.com"}}}),
    ], columns=jobs.CONFIG_COLUMNS)
    cost = pd.DataFrame([{"job_id": "1", "dbus": 10.0, "list_cost": 5.0, "all_purpose_dbus": 10.0,
                          "all_purpose_list_cost": 5.0, "currency": "USD"}])
    ctx.stats["jobs"] = {"job_runs": runs, "job_config": config, "job_cost": cost}
    jobs.evaluate(ctx)
    ids = {f.check_id: f for f in ctx.findings}
    assert ids["JOB-ALWAYS-FAILING"].evidence[0]["name"] == "broken"
    assert ids["JOB-ALL-PURPOSE"].evidence[0]["clusters"] == "c1"
    assert ids["JOB-ALL-PURPOSE"].evidence[0]["all_purpose_list_cost"] == 5.0
    assert [e["name"] for e in ids["JOB-SCHEDULED-NOT-RUNNING"].evidence] == ["stale"]
    assert {e["name"] for e in ids["JOB-NO-FAILURE-ALERT"].evidence} == {"broken", "stale"}
    assert {e["name"] for e in ids["JOB-RUN-AS-USER"].evidence} == {"broken", "stale"}
    assert "JOB-FAILURE-RATE" not in ids
    assert ctx.stats["jobs"]["summary"]["success_rate_pct"] == 60.0


def test_jobs_evaluate_with_no_data():
    ctx = _ctx()
    ctx.stats["jobs"] = {}
    jobs.evaluate(ctx)
    assert ctx.findings == []


# --- compute -------------------------------------------------------------------------

def test_classify_dbr():
    available = {"15.4.x-scala2.12", "16.1.x-scala2.12", "17.3.x-scala2.13"}
    lts = {"15.4.x-scala2.12", "17.3.x-scala2.13"}
    assert compute.classify_dbr("12.2.x-scala2.12", available, lts) == "unsupported"
    assert compute.classify_dbr("16.1.x-scala2.12", available, lts) == "non_lts"
    assert compute.classify_dbr("17.3.x-scala2.13", available, lts) == "ok"
    assert compute.classify_dbr("custom:abc", available, lts) == "unknown"
    assert compute.classify_dbr(None, available, lts) == "unknown"


def _cluster(**kw):
    base = {"cluster_id": "c", "cluster_name": "n", "owned_by": "a@x.com", "cluster_source": "UI",
            "dbr_version": "17.3.x-scala2.13", "data_security_mode": "SINGLE_USER", "policy_id": "p",
            "auto_termination_minutes": 30, "worker_count": 2, "min_autoscale_workers": None,
            "max_autoscale_workers": None, "tags": {"team": "x"}}
    base.update(kw)
    return base


def test_compute_evaluate_findings():
    ctx = _ctx()
    clusters = pd.DataFrame([
        _cluster(cluster_id="never", auto_termination_minutes=0, data_security_mode=None, policy_id=None, tags={}),
        _cluster(cluster_id="long", auto_termination_minutes=240, dbr_version="12.2.x-scala2.12"),
        _cluster(cluster_id="ok"),
        _cluster(cluster_id="job1", cluster_source="JOB", auto_termination_minutes=0, data_security_mode="NONE"),
    ])
    util = pd.DataFrame([
        {"cluster_id": "never", "node_hours": 700.0, "driver_hours": 700.0, "avg_cpu_pct": 4.0, "avg_mem_pct": 30.0},
        {"cluster_id": "ok", "node_hours": 5.0, "driver_hours": 5.0, "avg_cpu_pct": 50.0, "avg_mem_pct": 40.0},
    ])
    versions = pd.DataFrame([{"key": "17.3.x-scala2.13", "name": "17.3 LTS (Scala 2.13)"},
                             {"key": "16.4.x-scala2.12", "name": "16.4 (Scala 2.12)"}])
    warehouses = compute.warehouses_from_api([
        {"id": "w1", "name": "serverless", "enable_serverless_compute": True, "warehouse_type": "PRO",
         "auto_stop_mins": 10, "cluster_size": "Small"},
        {"id": "w2", "name": "pro-always-on", "enable_serverless_compute": False, "warehouse_type": "PRO",
         "auto_stop_mins": 0, "cluster_size": "Large"},
    ])
    cost = pd.DataFrame([{"compute_id": "never", "compute_kind": "cluster", "dbus": 100.0, "list_cost": 55.0,
                          "currency": "USD"},
                         {"compute_id": "w2", "compute_kind": "warehouse", "dbus": 10.0, "list_cost": 7.0,
                          "currency": "USD"}])
    ctx.stats["compute"] = {"clusters": clusters, "utilization": util, "spark_versions": versions,
                            "warehouses": warehouses, "cost": cost}
    compute.evaluate(ctx)
    ids = {f.check_id: f for f in ctx.findings}
    auto = ids["CMP-AUTOTERM"]
    assert auto.severity == "HIGH" and [e["cluster_id"] for e in auto.evidence] == ["never", "long"]
    assert auto.evidence[0]["list_cost"] == 55.0
    assert [e["cluster_id"] for e in ids["CMP-ACCESS-MODE"].evidence] == ["never"]  # job cluster excluded
    rt = ids["CMP-RUNTIME"]
    assert rt.severity == "HIGH" and rt.evidence[0]["id"] == "long" and rt.evidence[0]["status"] == "unsupported"
    assert [e["cluster_id"] for e in ids["CMP-ALWAYS-ON"].evidence] == ["never"]
    assert ids["CMP-LOW-UTIL"].severity == "MEDIUM" and ids["CMP-LOW-UTIL"].evidence[0]["cluster_id"] == "never"
    assert [e["cluster_id"] for e in ids["CMP-NO-POLICY"].evidence] == ["never"]
    assert [e["warehouse_id"] for e in ids["WH-AUTOSTOP"].evidence] == ["w2"] and ids["WH-AUTOSTOP"].severity == "HIGH"
    assert [e["warehouse_id"] for e in ids["WH-NOT-SERVERLESS"].evidence] == ["w2"]
    s = ctx.stats["compute"]["summary"]
    assert s["all_purpose_clusters"] == 3 and s["cluster_list_cost"] == 55.0 and s["warehouse_list_cost"] == 7.0


def test_runtime_check_includes_active_job_definitions_only():
    ctx = _ctx()
    old_cluster = [{"task_key": "t", "new_cluster": {"spark_version": "11.3.x-scala2.12"}}]
    ctx.stats["jobs"] = {"job_config": pd.DataFrame([
        jobs.parse_job_config({"job_id": 5, "settings": {"name": "old", "tasks": old_cluster,
                                                         "schedule": {"pause_status": "UNPAUSED"}}}),
        jobs.parse_job_config({"job_id": 6, "settings": {"name": "dormant", "tasks": old_cluster}}),
    ])}
    ctx.stats["compute"] = {"clusters": pd.DataFrame([_cluster()]),
                            "spark_versions": pd.DataFrame([{"key": "17.3.x-scala2.13", "name": "17.3 LTS"}])}
    compute.evaluate(ctx)
    rt = next(f for f in ctx.findings if f.check_id == "CMP-RUNTIME")
    assert rt.evidence == [{"kind": "job definition", "id": "5", "name": "old", "runtime": "11.3.x-scala2.12",
                            "status": "unsupported"}]
    assert ctx.stats["compute"]["dormant_jobs_with_runtime"] == 1 and "1 dormant job not listed" in rt.summary


def test_all_purpose_skips_dormant_jobs():
    ctx = _ctx()
    on_cluster = [{"task_key": "t", "existing_cluster_id": "c1"}]
    config = pd.DataFrame([
        jobs.parse_job_config({"job_id": 1, "settings": {"name": "ran", "tasks": on_cluster}}),
        jobs.parse_job_config({"job_id": 2, "settings": {"name": "dormant", "tasks": on_cluster}}),
    ], columns=jobs.CONFIG_COLUMNS)
    ctx.stats["jobs"] = {"job_config": config, "job_runs": pd.DataFrame([_run("1", 1, "SUCCEEDED")])}
    jobs.evaluate(ctx)
    ap = next(f for f in ctx.findings if f.check_id == "JOB-ALL-PURPOSE")
    assert [e["name"] for e in ap.evidence] == ["ran"] and ap.evidence[0]["ran_in_window"] is True
    assert "1 dormant job" in ap.summary


def test_decimals_to_float():
    from decimal import Decimal
    df = decimals_to_float(pd.DataFrame({"a": [Decimal("1.5"), None], "b": ["x", "y"]}))
    assert df["a"].tolist()[0] == 1.5 and df["b"].tolist() == ["x", "y"]


# --- settings (response shapes captured from a live workspace) ----------------------------------

LIVE_RAW = {
    "workspace_conf": {"enableTokensConfig": "true", "maxTokenLifetimeDays": "730", "enableIpAccessLists": "",
                       "enableDbfsFileBrowser": "true", "enableVerboseAuditLogs": "true"},
    "automatic_cluster_update": {"automatic_cluster_update_workspace": {"can_toggle": True, "enabled": True}},
    "compliance_security_profile": {"compliance_security_profile_workspace": {"is_enabled": False}},
    "enhanced_security_monitoring": {"enhanced_security_monitoring_workspace": {"is_enabled": False}},
    "default_namespace": {"namespace": {"value": "india_acceptance"}},
    "disable_legacy_access": {"disable_legacy_access": {"value": False}},
    "disable_legacy_dbfs": {"disable_legacy_dbfs": {"value": False}},
    "restrict_workspace_admins": {"restrict_workspace_admins": {"status": "ALLOW_ALL"}},
    "ip_access_lists": [],
    "tokens": [
        {"created_by_username": "a@x.com", "creation_time": 1_700_000_000_000, "expiry_time": -1},
        {"created_by_username": "b@x.com", "creation_time": 1_700_000_000_000,
         "expiry_time": 1_700_000_000_000 + 30 * settings.DAY_MS},
    ],
    "global_init_scripts": [{"name": "s1", "enabled": True}],
    "cluster_policies": [{"policy_id": "p"}],
    "admins": [{"value": str(i)} for i in range(14)],
}


def test_settings_inventory_and_findings():
    ctx = _ctx()
    settings.evaluate(ctx, LIVE_RAW)
    inv = ctx.stats["settings"]["inventory"].set_index("setting")
    assert inv.loc["IP access lists", "status"] == "review"
    assert inv.loc["Personal access token max lifetime", "current"] == "730 days"
    assert inv.loc["Automatic cluster update", "status"] == "ok"
    assert inv.loc["Compliance security profile", "status"] == "info"
    assert inv.loc["Default catalog", "status"] == "ok"
    ids = {f.check_id: f for f in ctx.findings}
    assert set(ids) == {"SET-IP-ACCESS", "SET-PAT", "SET-LEGACY", "SET-ADMINS", "SET-GLOBAL-INIT"}
    assert [e["owner"] for e in ids["SET-PAT"].evidence] == ["a@x.com"]  # only the token that never expires
    assert [e["setting"] for e in ids["SET-LEGACY"].evidence] == [
        "Disable legacy access (Hive metastore, no-isolation clusters)", "Disable legacy DBFS root and mounts",
        "DBFS file browser"]


def test_settings_unreadable_are_unknown_not_findings():
    ctx = _ctx()
    settings.evaluate(ctx, {k: None for k in LIVE_RAW})
    inv = ctx.stats["settings"]["inventory"]
    assert set(inv["status"]) == {"unknown"}
    assert ctx.findings == []


class FakeApi:
    def __init__(self, responses):
        self.responses, self.calls = responses, []

    def get(self, path, query):
        self.calls.append((path, dict(query)))
        r = self.responses.get(path)
        if isinstance(r, Exception):
            raise r
        return r


def test_settings_collect_uses_get_paths_and_tolerates_errors():
    ctx = _ctx()
    ctx.api = FakeApi({
        "/api/2.0/workspace-conf": LIVE_RAW["workspace_conf"],
        "/api/2.0/settings/types/disable_legacy_access/names/default": LIVE_RAW["disable_legacy_access"],
        "/api/2.0/token-management/tokens": PermissionError("403 not an admin"),
    })
    raw = settings.collect(ctx)
    assert raw["disable_legacy_access"] == LIVE_RAW["disable_legacy_access"]
    assert raw["tokens"] is None and any("tokens" in n for n in ctx.coverage_notes)
    assert ("/api/2.0/workspace-conf", {"keys": ",".join(settings.CONF_KEYS)}) in ctx.api.calls


def test_api_list_paginates():
    ctx = _ctx()
    pages = iter([{"jobs": [1, 2], "next_page_token": "t"}, {"jobs": [3]}])
    ctx.api = type("A", (), {"get": lambda self, p, q: next(pages)})()
    assert ctx.api_list("/api/2.1/jobs/list", "jobs") == [1, 2, 3]
