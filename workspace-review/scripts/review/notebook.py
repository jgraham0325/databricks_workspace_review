"""Drill-down notebook: one section per finding, showing every affected object and its fix.

The notebook reads the CSV files written next to the report, so it opens on any compute and never
re-runs the review. It is generated as Databricks source format and imported with the SDK.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Dict, List

from .context import Finding, ReviewContext, SEVERITY_ICON

CELL_SEP = "\n\n# COMMAND ----------\n\n"


def _md(text: str) -> str:
    return "\n".join("# MAGIC " + line if line else "# MAGIC" for line in ("%md\n" + text).splitlines())


def build_notebook_source(ctx: ReviewContext, findings: List[Finding], data_files: Dict[str, str],
                          run_dir: str, report_path: str, sql_path: str) -> str:
    """Notebook source. `data_files` maps check_id -> evidence CSV path (plus 'findings' and 'remediation')."""
    catalogs = ", ".join(ctx.config["include_catalogs"]) or "all catalogs"
    schemas = ", ".join(ctx.config["include_schemas"])
    scope = f"{catalogs}; schemas: {schemas}" if schemas else catalogs
    cells = [
        _md(f"# Workspace review drill-down\n\n"
            f"Generated {ctx.started_at.strftime('%Y-%m-%d %H:%M UTC')} by {ctx.current_user} · scope: {scope} · "
            f"activity window {ctx.config['activity_window_days']} days\n\n"
            f"- Report: `{report_path}`\n- Remediation SQL: `{sql_path}`\n\n"
            "**Run all** to load the tables (only reads the saved CSV files, takes seconds). Each finding below lists "
            "**every** affected object; use the table controls to sort, filter, or download. "
            "Nothing in this notebook changes the workspace: fix commands are shown as text, not run."),
        "\n".join([
            "import pandas as pd",
            f"DATA = {json.dumps(data_files, indent=2)}",
            "",
            "def show(check_id):",
            "    df = pd.read_csv(DATA[check_id])",
            "    print(f\"{len(df):,} affected objects\")",
            "    display(df)",
            "",
            "def show_fixes(check_id):",
            "    fixes = pd.read_csv(DATA['remediation'])",
            "    fixes = fixes[fixes['check_id'] == check_id].drop(columns='check_id')",
            "    print(f\"{len(fixes):,} statements (review before running; copy from here or the remediation file)\")",
            "    display(fixes)",
        ]),
        _md("## Summary of findings"),
        "display(pd.read_csv(DATA['findings']))",
    ]
    for f in findings:
        text = [f"## {SEVERITY_ICON[f.severity]} {f.severity} · {f.check_id}", f"### {f.title}", "", f.summary, "",
                f"**Impact:** {f.impact}", "", f"**Recommendation:** {f.recommendation}"]
        if f.doc_url:
            text += ["", f"**Docs:** {f.doc_url}"]
        cells.append(_md("\n".join(text)))
        if f.check_id in data_files:
            cells.append(f"show({json.dumps(f.check_id)})")
        if f.remediation_sql:
            cells.append(f"show_fixes({json.dumps(f.check_id)})")
    return "# Databricks notebook source\n" + CELL_SEP.join(cells) + "\n"


def write_notebook(ctx: ReviewContext, source: str, run_dir: str) -> str:
    """Import the notebook into the workspace. Falls back to a .py file if the SDK isn't available."""
    target = os.path.join(run_dir, "drilldown")
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat, Language
        api_path = target[len("/Workspace"):] if target.startswith("/Workspace/") else target
        w = WorkspaceClient()
        # A folder created through the /Workspace file system isn't always visible to the Workspace API yet,
        # so create it through the API too (no-op if it exists).
        w.workspace.mkdirs(os.path.dirname(api_path))
        w.workspace.import_(
            path=api_path, format=ImportFormat.SOURCE, language=Language.PYTHON,
            content=base64.b64encode(source.encode()).decode(), overwrite=True)
        return target
    except Exception as exc:
        ctx.record_error("drilldown notebook", "import", exc)
        # Not "drilldown.py": a file with that name blocks importing a notebook called "drilldown" next to it.
        path = target + "_source.py"
        with open(path, "w") as fh:
            fh.write(source)
        ctx.note(f"Drill-down notebook could not be imported, so its source was saved as a file ({path}). "
                 f"Import it as a notebook (Import > File) to use it.")
        return path
