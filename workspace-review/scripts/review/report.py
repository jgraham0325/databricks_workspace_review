"""Final step: health scores, the Markdown report, and the remediation SQL file.

The report is produced entirely from assets/report_template.md and the collected results, so its
structure is the same on every run. The Assistant should not write or restructure the report itself.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from . import notebook
from .checks import registered_areas
from .context import Finding, ReviewContext, SEVERITY_ICON, SEVERITY_ORDER
from .migration_backlog import CAUSES

# Areas shown in the score table, in order. Areas not built yet are shown as not assessed.
AREAS = [
    ("uc", "Unity Catalog data"),
    ("jobs", "Jobs & pipelines"),
    ("compute", "Compute & warehouses"),
    ("settings", "Workspace settings & security"),
    ("cost", "Cost"),
    ("governance", "Governance (grants)"),
]

MAIN_EVIDENCE_ROWS = 10
APPENDIX_EVIDENCE_ROWS = 50
REPORT_REMEDIATION_PER_FINDING = 25


# --- scores ------------------------------------------------------------------------

def compute_scores(ctx: ReviewContext) -> List[Dict[str, Any]]:
    weights = ctx.config["score_weights"]
    assessed_areas = registered_areas()
    rows = []
    for area, label in AREAS:
        findings = [f for f in ctx.findings if f.area == area]
        counts = {s: sum(f.severity == s for f in findings) for s in SEVERITY_ORDER}
        if area not in assessed_areas:
            rows.append({"area": area, "label": label, "score": None, "band": "Not assessed in this version",
                         **counts})
            continue
        if not ctx.stats.get(area):
            # Nothing was collected (skipped or failed): a score of 100 would be misleading.
            rows.append({"area": area, "label": label, "score": None, "band": "Not assessed (see Coverage)",
                         **counts})
            continue
        score = max(0, 100 - sum(weights[f.severity] for f in findings))
        band = next(name for floor, name in ctx.config["score_bands"] if score >= floor)
        rows.append({"area": area, "label": label, "score": score, "band": band, **counts})
    return rows


# --- writing -------------------------------------------------------------------------

def default_output_dir(ctx: ReviewContext) -> str:
    return f"/Workspace/Users/{ctx.current_user}/workspace_reviews"


def write_report(ctx: ReviewContext, output_dir: Optional[str] = None) -> Dict[str, str]:
    """Write one folder per run to the user's workspace folder:

        <output_dir>/<YYYY-MM-DD_HHMM>/
            report.md          fixed-structure summary (assets/report_template.md)
            remediation.sql    every generated fix command
            drilldown          notebook: every affected object per finding
            data/*.csv         full evidence per finding, findings summary, remediation statements

    This folder is the only thing the review writes.
    """
    run_dir = os.path.join(output_dir or default_output_dir(ctx), ctx.started_at.strftime("%Y-%m-%d_%H%M"))
    data_dir = os.path.join(run_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    report_path = os.path.join(run_dir, "report.md")
    sql_path = os.path.join(run_dir, "remediation.sql")
    findings = sorted_findings(ctx)

    data_files = write_data_files(findings, data_dir)
    with open(sql_path, "w") as f:
        f.write(render_remediation_sql(ctx))
    notebook_path = notebook.write_notebook(
        ctx, notebook.build_notebook_source(ctx, findings, data_files, run_dir, report_path, sql_path), run_dir)
    with open(report_path, "w") as f:
        f.write(render_report(ctx, remediation_file=sql_path, notebook_path=notebook_path, data_files=data_files))

    paths = {"folder": run_dir, "report": report_path, "remediation_sql": sql_path, "notebook": notebook_path}
    ctx.add_stat("report", "paths", paths)
    return paths


def sorted_findings(ctx: ReviewContext) -> List[Finding]:
    return sorted(ctx.findings, key=lambda f: (SEVERITY_ORDER[f.severity], -len(f.evidence)))


def write_data_files(findings: List[Finding], data_dir: str) -> Dict[str, str]:
    """Full evidence for every finding (no row limits) as CSV. Returns check_id -> path."""
    files = {}
    summary = pd.DataFrame([{"severity": f.severity, "check_id": f.check_id, "area": f.area, "title": f.title,
                             "affected_objects": len(f.evidence), "fix_commands": len(f.remediation_sql),
                             "impact": f.impact} for f in findings],
                           columns=["severity", "check_id", "area", "title", "affected_objects", "fix_commands",
                                    "impact"])
    files["findings"] = _to_csv(summary, os.path.join(data_dir, "findings.csv"))
    for f in findings:
        if f.evidence:
            files[f.check_id] = _to_csv(pd.DataFrame(f.evidence), os.path.join(data_dir, f"{f.check_id}.csv"))
    fixes = pd.DataFrame([{"check_id": f.check_id, "statement": s} for f in findings for s in f.remediation_sql],
                         columns=["check_id", "statement"])
    files["remediation"] = _to_csv(fixes, os.path.join(data_dir, "remediation.csv"))
    return files


def _to_csv(df: pd.DataFrame, path: str) -> str:
    # Lists (e.g. clustering columns) become "a, b" rather than Python reprs.
    df = df.apply(lambda col: col.map(lambda v: ", ".join(map(str, v)) if isinstance(v, (list, tuple)) else v))
    df.to_csv(path, index=False)
    return path


def render_report(ctx: ReviewContext, remediation_file: str, notebook_path: str = "",
                  data_files: Optional[Dict[str, str]] = None) -> str:
    with open(os.path.join(ctx.skill_dir, "assets", "report_template.md")) as f:
        template = f.read()
    template = re.sub(r"\A<!--.*?-->\s*", "", template, flags=re.S)

    findings = sorted_findings(ctx)
    data_files = data_files or {}
    scores = compute_scores(ctx)
    values = {
        "workspace_name": _workspace_name(ctx),
        "generated_at": ctx.started_at.strftime("%Y-%m-%d %H:%M UTC"),
        "catalog_scope": ", ".join(ctx.config["include_catalogs"]) or "all",
        "activity_window_days": str(ctx.config["activity_window_days"]),
        "current_user": ctx.current_user or "unknown",
        "score_rows": _score_rows(scores),
        "score_weights": ", ".join(f"{SEVERITY_ICON[s]} {w}" for s, w in ctx.config["score_weights"].items()),
        "top_findings": _top_findings(findings),
        "quick_wins": _quick_wins(findings),
        "notebook_path": notebook_path or "(not written)",
        "overview": _overview(ctx),
        "findings_by_area": _findings_by_area(findings, data_files),
        "migration_section": _migration_section(ctx, findings),
        "settings_section": _settings_section(ctx),
        "evidence": _evidence_appendix(findings),
        "remediation_file": remediation_file,
        "remediation_commands": _remediation_in_report(findings),
        "coverage": _coverage(ctx),
        "settings": _settings(ctx),
    }
    return fill_template(template, values)


def fill_template(template: str, values: Dict[str, str]) -> str:
    """Replace every placeholder in double braces. Fails loudly if the template and values disagree."""
    names = set(re.findall(r"\{\{(\w+)\}\}", template))
    missing, unused = names - set(values), set(values) - names
    if missing or unused:
        raise ValueError(f"Template mismatch. Missing values: {sorted(missing)}; unused values: {sorted(unused)}")
    return re.sub(r"\{\{(\w+)\}\}", lambda m: values[m.group(1)], template)


def render_remediation_sql(ctx: ReviewContext) -> str:
    lines = [
        f"-- Workspace review remediation commands: {_workspace_name(ctx)}",
        f"-- Generated {ctx.started_at.strftime('%Y-%m-%d %H:%M UTC')} by {ctx.current_user}",
        "-- NOTHING HERE HAS BEEN RUN. Review every statement before running it.",
        "-- Most OPTIMIZE statements rewrite whole tables: run them off-peak, largest tables first.",
        "",
    ]
    findings = sorted(ctx.findings, key=lambda f: SEVERITY_ORDER[f.severity])
    any_sql = False
    for f in findings:
        if not f.remediation_sql:
            continue
        any_sql = True
        lines += ["-- " + "=" * 76, f"-- [{f.severity}] {f.check_id}: {f.title}", "-- " + "=" * 76, ""]
        for stmt in f.remediation_sql:
            lines += [stmt, ""]
    if not any_sql:
        lines.append("-- No remediation commands were generated.")
    return "\n".join(lines) + "\n"


# --- sections --------------------------------------------------------------------------

def _workspace_name(ctx: ReviewContext) -> str:
    if ctx.workspace_url:
        return re.sub(r"^https?://", "", ctx.workspace_url).rstrip("/")
    return str(ctx.workspace_id or "unknown workspace")


def _score_rows(scores: List[Dict[str, Any]]) -> str:
    rows = []
    for s in scores:
        score = "—" if s["score"] is None else f"**{s['score']}**"
        rows.append(f"| {s['label']} | {score} | {s['band']} | {s['HIGH']} | {s['MEDIUM']} | {s['LOW']} |")
    return "\n".join(rows)


def _top_findings(findings: List[Finding]) -> str:
    if not findings:
        return "No problems found in the areas reviewed."
    return "\n".join(f"{i}. {SEVERITY_ICON[f.severity]} **{f.title}**: {f.impact}"
                     for i, f in enumerate(findings[:5], 1))


def _quick_wins(findings: List[Finding]) -> str:
    wins = [f for f in findings if f.remediation_sql and f.severity in ("HIGH", "MEDIUM")]
    if not wins:
        return "No ready-made fixes in this review."
    return "\n".join(f"- **{f.check_id}**: {len(f.remediation_sql):,} fix commands ready to review, one per "
                     f"affected object (drill-down notebook and remediation file)."
                     for f in wins)


def _overview(ctx: ReviewContext) -> str:
    parts = ["**Unity Catalog data**", _uc_overview(ctx), "**Jobs & pipelines**", _jobs_overview(ctx),
             "**Compute & warehouses**", _compute_overview(ctx)]
    scope = ", ".join(ctx.config["include_catalogs"])
    if scope:
        parts.insert(0, f"_The catalog filter ({scope}) applies to Unity Catalog data only; jobs, compute, and "
                        "settings cover the whole workspace._")
    return "\n\n".join(parts)


def _metric_table(rows) -> str:
    rows = [(k, v) for k, v in rows if v is not None]
    if not rows:
        return "_Not collected (see Coverage)._"
    return md_table(pd.DataFrame([{"Metric": k, "Value": _fmt(v)} for k, v in rows]))


def _jobs_overview(ctx: ReviewContext) -> str:
    j = ctx.stats.get("jobs", {})
    s = j.get("summary") or {}
    cur = s.get("currency") or ""
    pipes = j.get("pipelines_summary") or {}
    parts = [_metric_table([
        ("Jobs", s.get("jobs")), ("Jobs with an active schedule or trigger", s.get("jobs_with_active_trigger")),
        ("Jobs that ran in the window", s.get("jobs_that_ran")), ("Job runs in the window", s.get("runs")),
        ("Run success rate (%)", s.get("success_rate_pct")),
        (f"Job compute cost in the window (list price{', ' + cur if cur else ''})", s.get("job_list_cost")),
        ("Pipelines updated in the window", pipes.get("pipelines_updated")),
        ("Failed pipeline updates", pipes.get("failed_updates")),
    ])]
    top = j.get("top_cost_jobs")
    if top is not None and not top.empty:
        parts.append("**Most expensive jobs (list price)**\n\n" + md_table(top.round(2)))
    return "\n\n".join(parts)


def _compute_overview(ctx: ReviewContext) -> str:
    c = ctx.stats.get("compute", {})
    s = c.get("summary") or {}
    wh = c.get("warehouses")
    parts = [_metric_table([
        ("Clusters (not deleted)", s.get("clusters")), ("All-purpose clusters", s.get("all_purpose_clusters")),
        ("SQL warehouses", None if wh is None else len(wh)),
        ("Serverless SQL warehouses", None if wh is None else int(wh["is_serverless"].sum())),
        ("Cluster cost in the window (list price)", s.get("cluster_list_cost")),
        ("Warehouse cost in the window (list price)", s.get("warehouse_list_cost")),
    ])]
    rt = c.get("all_purpose_by_runtime")
    if rt:
        parts.append("**All-purpose clusters by runtime**\n\n" + md_table(pd.DataFrame(
            [{"Runtime": k, "Clusters": v} for k, v in sorted(rt.items(), key=lambda kv: -kv[1])])))
    ap = c.get("all_purpose_clusters")
    if ap is not None and not ap.empty and "list_cost" in ap and ap["list_cost"].notna().any():
        cols = [x for x in ["cluster_name", "owned_by", "dbr_version", "auto_termination_minutes", "avg_cpu_pct",
                            "list_cost"] if x in ap]
        parts.append("**Most expensive all-purpose clusters (list price)**\n\n"
                     + md_table(ap.sort_values("list_cost", ascending=False).head(10)[cols]))
    return "\n\n".join(parts)


def _settings_section(ctx: ReviewContext) -> str:
    inv = ctx.stats.get("settings", {}).get("inventory")
    if inv is None or inv.empty:
        return "_Workspace settings were not collected (see Coverage)._"
    icon = {"ok": "✅", "review": "⚠️", "unknown": "❔", "info": "ℹ️"}
    table = inv.assign(status=inv["status"].map(lambda s: f"{icon.get(s, '')} {s}"))
    parts = [md_table(table[["setting", "current", "recommended", "status", "note"]])]
    tokens = ctx.stats["settings"].get("token_summary")
    if tokens:
        parts.append(f"Personal access tokens in the workspace: {tokens['tokens']:,} ({tokens['flagged']:,} flagged).")
    parts.append("❔ unknown: not readable by the person running the review (most settings need a workspace admin).")
    return "\n\n".join(parts)


def _uc_overview(ctx: ReviewContext) -> str:
    uc = ctx.stats.get("uc", {})
    if not uc:
        return "_Unity Catalog inventory was not collected (see Coverage)._"
    parts = []
    counts = uc.get("counts", {})
    storage = uc.get("storage_totals", {})
    rows = [("Catalogs", counts.get("catalogs")), ("Schemas", counts.get("schemas")),
            ("Tables and views", counts.get("tables_and_views")), ("Volumes", counts.get("volumes")),
            ("Delta tables inspected", storage.get("tables_inspected")),
            ("Total size (GB)", storage.get("total_size_gb")), ("Total files", storage.get("total_files")),
            ("Liquid-clustered tables", storage.get("clustered_tables")),
            ("Partitioned tables", storage.get("partitioned_tables")),
            ("Tables without a description (%)", uc.get("pct_tables_without_comment"))]
    hive = uc.get("hive_summary")
    if hive:
        rows.append(("Tables still in hive_metastore", hive["hive_tables"]))
    parts.append(md_table(pd.DataFrame([{"Metric": k, "Value": _fmt(v)} for k, v in rows if v is not None])))

    for title, key in [("Tables by type", "tables_by_type"), ("Tables by format", "tables_by_format"),
                       ("Tables per catalog", "tables_per_catalog")]:
        data = uc.get(key)
        if data:
            parts.append(f"**{title}**\n\n" + md_table(pd.DataFrame(
                [{"": k, "Tables": v} for k, v in sorted(data.items(), key=lambda kv: -kv[1])])))
    largest = uc.get("largest_tables")
    if largest is not None and not largest.empty:
        parts.append("**Largest tables**\n\n" + md_table(largest.head(10)[
            ["full_name", "size_gb", "num_files", "avg_file_mb", "clustering_columns", "partition_columns"]]))
    return "\n\n".join(parts)


def _findings_by_area(findings: List[Finding], data_files: Dict[str, str]) -> str:
    if not findings:
        return "No findings."
    out = []
    for area, label in AREAS:
        area_findings = [f for f in findings if f.area == area]
        if not area_findings:
            continue
        out.append(f"### {label}")
        for f in area_findings:
            block = [f"#### {SEVERITY_ICON[f.severity]} {f.severity} · {f.check_id}: {f.title}", "", f.summary, "",
                     f"**Impact:** {f.impact}", "", f"**Recommendation:** {f.recommendation}"]
            if f.doc_url:
                block += ["", f"**Docs:** {f.doc_url}"]
            if f.evidence:
                block += ["", f"**Affected objects: {len(f.evidence):,}.** All of them are in the drill-down "
                              f"notebook and `{data_files.get(f.check_id, f'data/{f.check_id}.csv')}`."]
                more = len(f.evidence) - MAIN_EVIDENCE_ROWS
                block += ["", md_table(pd.DataFrame(f.evidence[:MAIN_EVIDENCE_ROWS]))]
                if more > 0:
                    block += ["", f"_Top {MAIN_EVIDENCE_ROWS} shown; {more:,} more in the drill-down notebook._"]
            out.append("\n".join(block))
    return "\n\n".join(out)


def _migration_section(ctx: ReviewContext, findings: List[Finding]) -> str:
    uc = ctx.stats.get("uc", {})
    parts = []
    hive = uc.get("hive_summary")
    if hive:
        parts.append(f"- Tables still in `hive_metastore`: **{hive['hive_tables']}** "
                     f"({hive['with_known_uc_copy']} with a known Unity Catalog copy, "
                     f"{hive['without_known_uc_copy']} without)")
    elif not ctx.catalog_in_scope("hive_metastore"):
        parts.append("- `hive_metastore` was not in the review scope.")
    elif not ctx.sources.get("hive_metastore"):
        parts.append("- `hive_metastore` was not readable.")

    mig = next((f for f in findings if f.check_id == "MIG-BACKLOG"), None)
    if mig is None:
        parts.append("- No tables migrated from `hive_metastore` with a small-file backlog were found.")
        return "\n".join(parts)

    ev = pd.DataFrame(mig.evidence)
    parts.append(f"- Migrated tables with a small-file backlog: **{len(ev)}** ({SEVERITY_ICON[mig.severity]} "
                 f"{mig.severity})")
    rewritten = ev["rewritten_pct"].dropna() if "rewritten_pct" in ev else pd.Series(dtype=float)
    if not rewritten.empty:
        parts.append(f"- Share of migrated files rewritten by maintenance so far: median **{rewritten.median():.0f}%**")
    parts.append("\n**Why this happens.** A deep clone copies the Hive table's files as they are, so a fragmented "
                 "Hive table becomes a fragmented Unity Catalog table. Ongoing maintenance mostly handles new data, "
                 "so the migrated files stay as they were until a one-time full OPTIMIZE rewrites them.")
    causes = ev["cause"].value_counts()
    parts.append("\n**Likely causes**\n\n" + md_table(pd.DataFrame(
        [{"Cause": c, "Tables": n, "Explanation": CAUSES[c]} for c, n in causes.items()])))
    cols = [c for c in ["table", "migrated_via", "files", "files_at_migration", "rewritten_pct", "avg_file_mb",
                        "size_gb", "po_enabled", "cause"] if c in ev]
    parts.append("\n**Tables (most files first)**\n\n" + md_table(ev[cols].head(20)))
    if len(ev) > 20:
        parts.append(f"\n_…and {len(ev) - 20} more in the appendix._")
    return "\n".join(parts)


def _evidence_appendix(findings: List[Finding]) -> str:
    blocks = []
    for f in findings:
        if not f.evidence:
            continue
        block = [f"#### {f.check_id} ({len(f.evidence)} objects)", "",
                 md_table(pd.DataFrame(f.evidence[:APPENDIX_EVIDENCE_ROWS]))]
        if len(f.evidence) > APPENDIX_EVIDENCE_ROWS:
            block += ["", f"_Showing {APPENDIX_EVIDENCE_ROWS} of {len(f.evidence):,}; the full list is in the "
                          f"drill-down notebook._"]
        blocks.append("\n".join(block))
    return "\n\n".join(blocks) or "No evidence recorded."


def _remediation_in_report(findings: List[Finding]) -> str:
    blocks = []
    for f in findings:
        if not f.remediation_sql:
            continue
        shown = f.remediation_sql[:REPORT_REMEDIATION_PER_FINDING]
        block = [f"#### {f.check_id}", "", "```sql", "\n\n".join(shown), "```"]
        if len(f.remediation_sql) > len(shown):
            block += ["", f"_{len(f.remediation_sql) - len(shown)} more in the remediation file._"]
        blocks.append("\n".join(block))
    return "\n\n".join(blocks) or "No remediation commands were generated."


def _coverage(ctx: ReviewContext) -> str:
    parts = [md_table(pd.DataFrame([{"Source": k, "Readable": "✅" if v else "❌"}
                                    for k, v in ctx.sources.items()]))] if ctx.sources else []
    if ctx.coverage_notes:
        parts.append("**Gaps**\n\n" + "\n".join(f"- {n}" for n in ctx.coverage_notes))
    else:
        parts.append("No coverage gaps.")
    parts.append("Objects are only visible if the person running the review has privileges on them.")
    if ctx.errors:
        parts.append(f"**Errors on individual objects: {len(ctx.errors)}** (first 20)\n\n"
                     + md_table(pd.DataFrame(ctx.errors[:20])))
    return "\n\n".join(parts)


def _settings(ctx: ReviewContext) -> str:
    keys = ["include_catalogs", "excluded_catalogs", "extra_excluded_catalogs", "activity_window_days",
            "max_tables_detail", "history_min_files", "small_file_avg_mb", "small_files_min_table_gb",
            "small_files_medium_count", "small_files_high_count", "migration_rewrite_ratio_high", "deep_scan"]
    return md_table(pd.DataFrame([{"Setting": k, "Value": _fmt(ctx.config[k])} for k in keys]))


# --- markdown helpers --------------------------------------------------------------------

def _fmt(value: Any) -> str:
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".") if value != int(value) else f"{int(value):,}"
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value) if value else "—"
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def md_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_None._"

    def cell(v):
        return _fmt(v).replace("|", "\\|").replace("\n", " ")

    header = "| " + " | ".join(str(c) for c in df.columns) + " |"
    sep = "|" + "|".join("---" for _ in df.columns) + "|"
    body = ["| " + " | ".join(cell(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join([header, sep] + body)
