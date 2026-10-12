"""Report, remediation file, and drill-down notebook generation (no Spark, no workspace)."""

import os
import sys

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_DIR = os.path.join(ROOT, "workspace-review")

from review import migration_backlog as mb  # noqa: E402
from review import report  # noqa: E402
from review.context import Finding, ReviewContext  # noqa: E402

GB = 1024 ** 3
N_TABLES = 300


@pytest.fixture(autouse=True)
def no_sdk(monkeypatch):
    # Never let tests reach a real workspace through the SDK's default auth.
    monkeypatch.setitem(sys.modules, "databricks.sdk", None)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.workspace", None)


def _ctx_with_backlog():
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR, include_catalogs=["main"])
    ctx.current_user = "someone@example.com"
    ctx.workspace_url = "https://adb-1.azuredatabricks.net"
    ctx.sources = {"information_schema": True, "billing": False, "hive_metastore": True}
    ctx.note("`billing` not readable: Cost analysis skipped.")
    detail = pd.DataFrame([{
        "full_name": f"main.sales.t{i:04d}", "name_parts": ("main", "sales", f"t{i:04d}"),
        "num_files": 200_000 - i, "size_bytes": 300 * GB, "avg_file_mb": 1.5,
        "partition_columns": [], "clustering_columns": ["id"] if i % 2 else [], "partition_count": None,
        "properties": {}, "history_versions_read": 50, "migration_method": "deep_clone",
        "clone_source_is_hive": True, "clone_source": f"hive_metastore.sales.t{i:04d}", "migration_version": 0,
        "clone_files": 200_000, "optimize_runs_after_migration": 3, "optimize_files_removed_after_migration": 1000,
        "optimize_predicates": [], "recluster_needed": i % 2 == 1, "po_enabled": True,
    } for i in range(N_TABLES)])
    ctx.add_stat("uc", "table_detail", detail)
    ctx.add_stat("uc", "counts", {"catalogs": 1, "schemas": 1, "tables_and_views": N_TABLES, "volumes": 0})
    mb.run(ctx)
    ctx.add_finding(Finding("UC-NO-COMMENT", "uc", "LOW", "90% of tables have no description", "s",
                            "i", "r", evidence=[{"full_name": "main.sales.t0000"}]))
    return ctx


def test_scores():
    ctx = _ctx_with_backlog()
    scores = {s["area"]: s for s in report.compute_scores(ctx)}
    assert scores["uc"]["score"] == 100 - 20 - 3  # one HIGH (MIG-BACKLOG), one LOW
    assert scores["uc"]["band"] == "Needs attention"  # Good starts at 85
    assert scores["jobs"]["score"] is None and scores["jobs"]["band"].startswith("Not assessed")


def test_write_report_folder_has_everything(tmp_path):
    ctx = _ctx_with_backlog()
    paths = report.write_report(ctx, output_dir=str(tmp_path))
    run_dir = paths["folder"]
    assert os.path.dirname(run_dir) == str(tmp_path)
    assert sorted(os.listdir(run_dir)) == ["data", "drilldown_source.py", "remediation.sql", "report.md"]

    # Full evidence, no row limit.
    mig = pd.read_csv(os.path.join(run_dir, "data", "MIG-BACKLOG.csv"))
    assert len(mig) == N_TABLES
    assert mig["files"].dtype.kind == "i"  # numeric, so it sorts correctly
    assert mig.loc[0, "fix_command"] == "OPTIMIZE `main`.`sales`.`t0000`;"
    assert mig.loc[1, "fix_command"] == "OPTIMIZE `main`.`sales`.`t0001` FULL;"
    fixes = pd.read_csv(os.path.join(run_dir, "data", "remediation.csv"))
    assert (fixes["check_id"] == "MIG-BACKLOG").sum() == N_TABLES

    sql = open(paths["remediation_sql"]).read()
    assert sql.count("OPTIMIZE `main`.`sales`.") == N_TABLES
    assert "NOTHING HERE HAS BEEN RUN" in sql


def test_report_follows_template(tmp_path):
    ctx = _ctx_with_backlog()
    paths = report.write_report(ctx, output_dir=str(tmp_path))
    md = open(paths["report"]).read()
    headings = [line for line in md.splitlines() if line.startswith("## ")]
    assert headings == ["## Executive summary", "## Workspace overview", "## Findings by area",
                        "## Hive → Unity Catalog migration health", "## Workspace settings",
                        "## Technical appendix"]
    assert "{{" not in md and "<!--" not in md
    assert "| Unity Catalog data | **77** | Needs attention | 1 | 0 | 1 |" in md
    assert f"**Affected objects: {N_TABLES:,}.**" in md
    assert paths["notebook"] in md
    assert "`billing` not readable" in md


def test_notebook_source(tmp_path):
    ctx = _ctx_with_backlog()
    paths = report.write_report(ctx, output_dir=str(tmp_path))
    src = open(paths["notebook"]).read()
    assert src.startswith("# Databricks notebook source\n")
    assert 'show("MIG-BACKLOG")' in src and 'show_fixes("MIG-BACKLOG")' in src
    assert 'show("UC-NO-COMMENT")' in src and 'show_fixes("UC-NO-COMMENT")' not in src
    # MIG-BACKLOG (HIGH) comes before UC-NO-COMMENT (LOW).
    assert src.index("MIG-BACKLOG") < src.index("UC-NO-COMMENT")
    # Markdown cells must be fully MAGIC-prefixed.
    for cell in src.split("# COMMAND ----------"):
        lines = [line for line in cell.strip().splitlines() if line and not line.startswith("# Databricks")]
        if lines and lines[0].startswith("# MAGIC %md"):
            assert all(line.startswith("# MAGIC") for line in lines)
    compile(src, "drilldown_source.py", "exec")  # valid Python (display() is only resolved at run time)
    assert any("could not be imported" in n for n in ctx.coverage_notes)  # no SDK in tests


def test_fill_template_is_strict():
    with pytest.raises(ValueError, match="Missing values"):
        report.fill_template("{{a}} {{b}}", {"a": "1"})
    with pytest.raises(ValueError, match="unused"):
        report.fill_template("{{a}}", {"a": "1", "b": "2"})
    assert report.fill_template("x {{a}} y", {"a": "{not a placeholder}"}) == "x {not a placeholder} y"


def test_report_with_no_findings(tmp_path):
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR)
    ctx.current_user = "someone@example.com"
    paths = report.write_report(ctx, output_dir=str(tmp_path))
    md = open(paths["report"]).read()
    assert "No problems found" in md
    assert "No remediation commands were generated." in open(paths["remediation_sql"]).read()


def test_fmt_keeps_two_decimals():
    assert report._fmt(0.25) == "0.25"
    assert report._fmt(1.5) == "1.5"
    assert report._fmt(3.0) == "3"
    assert report._fmt(1234567) == "1,234,567"
