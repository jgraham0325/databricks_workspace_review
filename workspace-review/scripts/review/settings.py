"""Workspace settings: security and administration settings compared with recommended values.

Everything is read through GET endpoints. Settings an admin can't read (or that this workspace tier doesn't
have) are listed as "unknown" rather than treated as problems. REST paths confirmed on a live workspace (2026-10).
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from .checks import CheckSpec
from .context import Finding, ReviewContext, evidence_records, plural

AREA = "settings"
CONF_KEYS = ["enableTokensConfig", "maxTokenLifetimeDays", "enableIpAccessLists", "enableDbfsFileBrowser",
             "enableVerboseAuditLogs", "enableWebTerminal", "enforceUserIsolation", "enableResultsDownloading",
             "enableExportNotebook", "enableDeprecatedGlobalInitScripts"]
SETTINGS = {  # name -> settings API type
    "automatic_cluster_update": "automatic_cluster_update",
    "compliance_security_profile": "shield_csp_enablement_ws_db",
    "enhanced_security_monitoring": "shield_esm_enablement_ws_db",
    "default_namespace": "default_namespace_ws",
    "disable_legacy_access": "disable_legacy_access",
    "disable_legacy_dbfs": "disable_legacy_dbfs",
    "restrict_workspace_admins": "restrict_workspace_admins",
}
DAY_MS = 24 * 3600 * 1000


def run(ctx: ReviewContext) -> None:
    raw = collect(ctx)
    ctx.add_stat(AREA, "raw", raw)
    evaluate(ctx, raw)


# --- collection --------------------------------------------------------------------

def collect(ctx: ReviewContext) -> Dict[str, Any]:
    raw: Dict[str, Any] = {}

    def fetch(name: str, fn: Callable[[], Any]) -> None:
        try:
            raw[name] = fn()
        except Exception as exc:
            raw[name] = None
            ctx.record_error(name, "settings", exc)

    fetch("workspace_conf", lambda: ctx.api_get("/api/2.0/workspace-conf", {"keys": ",".join(CONF_KEYS)}))
    for name, type_ in SETTINGS.items():
        fetch(name, lambda t=type_: ctx.api_get(f"/api/2.0/settings/types/{t}/names/default"))
    fetch("ip_access_lists", lambda: ctx.api_get("/api/2.0/ip-access-lists").get("ip_access_lists") or [])
    fetch("tokens", lambda: ctx.api_get("/api/2.0/token-management/tokens").get("token_infos") or [])
    fetch("global_init_scripts", lambda: ctx.api_get("/api/2.0/global-init-scripts").get("scripts") or [])
    fetch("cluster_policies", lambda: ctx.api_get("/api/2.0/policies/clusters/list").get("policies") or [])
    fetch("admins", lambda: (ctx.api_get("/api/2.0/preview/scim/v2/Groups",
                                         {"filter": 'displayName eq "admins"', "attributes": "members"})
                             .get("Resources") or [{}])[0].get("members") or [])
    unreadable = [k for k, v in raw.items() if v is None]
    if unreadable:
        ctx.note(f"Settings not readable (needs workspace admin, or not available on this tier): {', '.join(unreadable)}")
    return raw


# --- pure logic (unit tested) -----------------------------------------------------------

def setting_value(raw: Dict[str, Any], name: str) -> Any:
    """Pull the meaningful value out of each settings API response shape."""
    r = raw.get(name)
    if not r:
        return None
    if name == "automatic_cluster_update":
        return (r.get("automatic_cluster_update_workspace") or {}).get("enabled")
    if name == "compliance_security_profile":
        return (r.get("compliance_security_profile_workspace") or {}).get("is_enabled")
    if name == "enhanced_security_monitoring":
        return (r.get("enhanced_security_monitoring_workspace") or {}).get("is_enabled")
    if name == "default_namespace":
        return (r.get("namespace") or {}).get("value")
    if name in ("disable_legacy_access", "disable_legacy_dbfs"):
        return (r.get(name) or {}).get("value")
    if name == "restrict_workspace_admins":
        return (r.get("restrict_workspace_admins") or {}).get("status")
    return None


def conf(raw: Dict[str, Any], key: str) -> Optional[str]:
    """workspace-conf value as a lowercase string; '' means not set (the default applies), None means unreadable."""
    c = raw.get("workspace_conf")
    if c is None:
        return None
    return str(c.get(key) or "").strip().lower()


def token_rows(tokens: List[Dict[str, Any]], max_days: int, now_ms: Optional[int] = None) -> pd.DataFrame:
    """Personal access tokens that never expire, live longer than max_days, or haven't been used for 90 days."""
    now_ms = now_ms or int(time.time() * 1000)
    rows = []
    for t in tokens:
        created, expiry = t.get("creation_time"), t.get("expiry_time")
        never = expiry in (None, -1, 0)
        lifetime = None if never or not created else round((expiry - created) / DAY_MS)
        last_used = t.get("last_used_day")
        idle_days = round((now_ms - last_used) / DAY_MS) if isinstance(last_used, (int, float)) and last_used > 0 else None
        reasons = []
        if never:
            reasons.append("never expires")
        elif lifetime is not None and lifetime > max_days:
            reasons.append(f"lifetime {lifetime} days")
        if idle_days is not None and idle_days > 90:
            reasons.append(f"unused for {idle_days} days")
        if reasons:
            rows.append({"owner": t.get("created_by_username") or t.get("owner_id"), "comment": t.get("comment"),
                         "created": pd.to_datetime(created, unit="ms", utc=True) if created else None,
                         "expires": None if never else pd.to_datetime(expiry, unit="ms", utc=True),
                         "issue": ", ".join(reasons)})
    return pd.DataFrame(rows, columns=["owner", "comment", "created", "expires", "issue"])


def inventory(raw: Dict[str, Any], cfg: Dict[str, Any]) -> pd.DataFrame:
    """One row per setting: current value, recommended value, and status (ok / review / unknown / info)."""
    rows = []

    def add(setting, value, recommended, ok: Optional[bool], note=""):
        status = "unknown" if value is None else ("info" if ok is None else ("ok" if ok else "review"))
        rows.append({"setting": setting, "current": _show(value), "recommended": recommended, "status": status,
                     "note": note})

    ip = conf(raw, "enableIpAccessLists")
    lists = raw.get("ip_access_lists")
    add("IP access lists", None if ip is None else ("enabled" if ip == "true" else "not enabled"), "enabled",
        None if ip is None else ip == "true",
        f"{len(lists)} list(s) defined" if lists is not None else "Not needed if access is restricted by Private Link.")

    tokens_on, lifetime = conf(raw, "enableTokensConfig"), conf(raw, "maxTokenLifetimeDays")
    if tokens_on is None:
        add("Personal access token max lifetime", None, f"≤ {cfg['token_max_lifetime_days']} days", None)
    elif tokens_on == "false":
        add("Personal access tokens", "disabled", "disabled or time-limited", True)
    else:
        days = int(lifetime) if lifetime and lifetime.isdigit() else None
        add("Personal access token max lifetime", f"{days} days" if days else "no limit",
            f"≤ {cfg['token_max_lifetime_days']} days", days is not None and days <= cfg["token_max_lifetime_days"])

    for name, label, want, note in [
        ("disable_legacy_access", "Disable legacy access (Hive metastore, no-isolation clusters)", True,
         "Turn on once Hive tables are migrated to Unity Catalog."),
        ("disable_legacy_dbfs", "Disable legacy DBFS root and mounts", True, ""),
        ("automatic_cluster_update", "Automatic cluster update", True, "Applies OS and security patches on schedule."),
    ]:
        v = setting_value(raw, name)
        add(label, v, _show(want), None if v is None else v == want, note)

    for name, label in [("compliance_security_profile", "Compliance security profile"),
                        ("enhanced_security_monitoring", "Enhanced security monitoring")]:
        v = setting_value(raw, name)
        add(label, v, "on if subject to compliance requirements (e.g. HIPAA, PCI)", None)

    audit = conf(raw, "enableVerboseAuditLogs")
    add("Verbose audit logs", None if audit is None else audit == "true", "on", None if audit is None else audit == "true",
        "Logs notebook and SQL commands in audit logs.")
    dbfs_browser = conf(raw, "enableDbfsFileBrowser")
    add("DBFS file browser", None if dbfs_browser is None else dbfs_browser == "true", "off",
        None if dbfs_browser is None else dbfs_browser != "true")
    add("Restrict workspace admins", setting_value(raw, "restrict_workspace_admins"), "depends on admin model", None)
    add("Default catalog", setting_value(raw, "default_namespace"), "a governed catalog, not hive_metastore",
        None if setting_value(raw, "default_namespace") is None
        else setting_value(raw, "default_namespace") != "hive_metastore")

    admins = raw.get("admins")
    add("Workspace admins", None if admins is None else len(admins), f"≤ {cfg['admins_max_members']}",
        None if admins is None else len(admins) <= cfg["admins_max_members"],
        "Members of the admins group (users, service principals, and groups).")
    scripts = raw.get("global_init_scripts")
    enabled_scripts = None if scripts is None else [s for s in scripts if s.get("enabled")]
    add("Enabled global init scripts", None if scripts is None else len(enabled_scripts), "0, or reviewed",
        None if scripts is None else not enabled_scripts, "Run on every cluster in the workspace.")
    policies = raw.get("cluster_policies")
    add("Cluster policies", None if policies is None else len(policies), "≥ 1 custom policy", None)
    return pd.DataFrame(rows)


def _show(v: Any) -> Any:
    if isinstance(v, bool):
        return "on" if v else "off"
    return v


def evaluate(ctx: ReviewContext, raw: Dict[str, Any]) -> None:
    ctx.add_stat(AREA, "raw", raw)
    prepare(ctx)
    for check in CHECKS:
        check.evaluate(ctx)


def prepare(ctx: ReviewContext) -> None:
    raw = ctx.stats.get(AREA, {}).get("raw", {})
    cfg = ctx.config
    inv = inventory(raw, cfg)
    ctx.add_stat(AREA, "inventory", inv)
    tokens = raw.get("tokens")
    bad_tokens = token_rows(tokens, cfg["token_max_lifetime_days"]) if tokens is not None else pd.DataFrame()
    ctx.add_stat(AREA, "bad_tokens", bad_tokens)
    ctx.add_stat(AREA, "token_summary", None if tokens is None else {"tokens": len(tokens), "flagged": len(bad_tokens)})


def _evaluate_check(ctx: ReviewContext, only: str) -> None:
    raw = ctx.stats.get(AREA, {}).get("raw", {})
    inv = ctx.stats.get(AREA, {}).get("inventory")
    if inv is None:
        return
    cfg = ctx.config
    status = dict(zip(inv["setting"], inv["status"]))

    if only == "SET-IP-ACCESS" and status.get("IP access lists") == "review":
        ctx.add_finding(Finding(
            check_id="SET-IP-ACCESS", area=AREA, severity="MEDIUM",
            title="IP access lists are not enabled",
            summary="The workspace accepts connections from any IP address with valid credentials.",
            impact="A leaked token or password can be used from anywhere.",
            recommendation="Enable IP access lists for corporate network ranges, unless front-end Private Link "
                           "already restricts access.",
            doc_url="https://docs.databricks.com/security/network/front-end/ip-access-list.html",
        ))

    bad_tokens = ctx.stats[AREA].get("bad_tokens", pd.DataFrame())
    lifetime_bad = inv.loc[inv["setting"] == "Personal access token max lifetime", "status"].eq("review").any()
    if only == "SET-PAT" and (lifetime_bad or not bad_tokens.empty):
        current = inv.loc[inv["setting"] == "Personal access token max lifetime", "current"]
        ctx.add_finding(Finding(
            check_id="SET-PAT", area=AREA, severity="MEDIUM",
            title="Personal access tokens are long-lived"
                  + (f" ({plural(len(bad_tokens), 'token')} to review)" if not bad_tokens.empty else ""),
            summary=f"Maximum token lifetime: {current.iloc[0] if len(current) else 'unknown'}. Flagged tokens never "
                    f"expire, live longer than {cfg['token_max_lifetime_days']} days, or haven't been used for 90 days.",
            impact="Long-lived tokens are a common route for unauthorised access if leaked.",
            recommendation=f"Set the maximum token lifetime to {cfg['token_max_lifetime_days']} days or less, revoke "
                           "unused tokens, and use OAuth with service principals for automation.",
            evidence=evidence_records(bad_tokens),
            doc_url="https://docs.databricks.com/admin/access-control/tokens.html",
        ))

    legacy = inv[(inv["setting"].str.startswith("Disable legacy") | (inv["setting"] == "DBFS file browser"))
                 & (inv["status"] == "review")]
    if only == "SET-LEGACY" and not legacy.empty:
        ctx.add_finding(Finding(
            check_id="SET-LEGACY", area=AREA, severity="MEDIUM",
            title=f"{plural(len(legacy), 'legacy data access feature')} still enabled",
            summary="Legacy Hive metastore access, no-isolation clusters, the DBFS root and mounts, or the DBFS file "
                    "browser are still allowed.",
            impact="Data can still be read and written outside Unity Catalog governance and auditing.",
            recommendation="After migrating remaining Hive tables (see the migration section), turn on 'Disable legacy "
                           "access' and 'Disable legacy DBFS', and turn off the DBFS file browser.",
            evidence=legacy[["setting", "current", "recommended", "note"]].to_dict("records"),
            doc_url="https://docs.databricks.com/aws/en/admin/account-settings/legacy-features",
        ))

    for setting, check_id, sev, title, impact, rec in [
        ("Workspace admins", "SET-ADMINS", "LOW", "More workspace admins than recommended",
         "Every admin can change security settings and read all data in the workspace.",
         "Keep the admins group small; give other users targeted permissions instead."),
        ("Automatic cluster update", "SET-AUTO-UPDATE", "LOW", "Automatic cluster update is off",
         "Long-running clusters miss OS and security patches.", "Enable automatic cluster update with a maintenance window."),
        ("Enabled global init scripts", "SET-GLOBAL-INIT", "LOW", "Global init scripts are enabled",
         "They run on every cluster, so a mistake breaks every cluster and they're hard to audit.",
         "Review each script; move cluster-specific setup into cluster policies or cluster-scoped init scripts."),
        ("Verbose audit logs", "SET-AUDIT", "LOW", "Verbose audit logs are off",
         "Notebook and SQL commands aren't recorded in audit logs.", "Enable verbose audit logs."),
        ("Default catalog", "SET-DEFAULT-CATALOG", "LOW", "Default catalog is hive_metastore",
         "Unqualified table names land in the legacy Hive metastore.", "Set the default catalog to a Unity Catalog catalog."),
    ]:
        if check_id != only:
            continue
        if status.get(setting) != "review":
            continue
        row = inv[inv["setting"] == setting].iloc[0]
        ctx.add_finding(Finding(
            check_id=check_id, area=AREA, severity=sev, title=title,
            summary=f"Current: {row['current']}. Recommended: {row['recommended']}.",
            impact=impact, recommendation=rec,
            evidence=[{"setting": setting, "current": row["current"], "recommended": row["recommended"]}],
        ))


CHECKS = (
    CheckSpec("SET-IP-ACCESS", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-IP-ACCESS")),
    CheckSpec("SET-PAT", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-PAT"),
              ("token_max_lifetime_days",)),
    CheckSpec("SET-LEGACY", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-LEGACY")),
    CheckSpec("SET-ADMINS", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-ADMINS"),
              ("admins_max_members",)),
    CheckSpec("SET-AUTO-UPDATE", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-AUTO-UPDATE")),
    CheckSpec("SET-GLOBAL-INIT", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-GLOBAL-INIT")),
    CheckSpec("SET-AUDIT", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-AUDIT")),
    CheckSpec("SET-DEFAULT-CATALOG", AREA, "settings", lambda ctx: _evaluate_check(ctx, "SET-DEFAULT-CATALOG")),
)
