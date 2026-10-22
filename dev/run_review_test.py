# Databricks notebook source
# Test harness for the workspace-review skill: runs the review as SKILL.md describes, plus diagnostics.
import sys, time, json, traceback
dbutils.widgets.text("skill_dir", "/Workspace/Users/<you>/.assistant/skills/workspace-review")
dbutils.widgets.text("include", "")
dbutils.widgets.text("schemas", "")
dbutils.widgets.text("overrides", "{}")
SKILL_DIR = dbutils.widgets.get("skill_dir").strip()
if "<you>" in SKILL_DIR:
    raise ValueError("Set the skill_dir job parameter to the installed workspace-review skill path.")
sys.path.insert(0, f"{SKILL_DIR}/scripts")
for m in [m for m in sys.modules if m == "review" or m.startswith("review.")]:
    del sys.modules[m]
import review

INCLUDE = [c.strip() for c in dbutils.widgets.get("include").split(",") if c.strip()]
SCHEMAS = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
OVERRIDES = json.loads(dbutils.widgets.get("overrides"))
ctx = review.ReviewContext.create(spark, SKILL_DIR, include_catalogs=INCLUDE, include_schemas=SCHEMAS,
                                  activity_window_days=30, overrides=OVERRIDES)

# Time each step (same order and error handling as run_all).
timings = {}
for name, step in review.steps_to_run(ctx):
    t = time.time()
    try:
        step(ctx)
    except Exception as exc:
        ctx.record_error(name, "step", exc)
        ctx.note(f"Step `{name}` failed: {traceback.format_exc()[-1500:]}")
    timings[name] = round(time.time() - t, 1)
t = time.time()
try:
    paths = review.write_report(ctx)
except Exception:
    paths = {"error": traceback.format_exc()[-3000:]}
timings["write_report"] = round(time.time() - t, 1)
display(review.findings_table(ctx))

# COMMAND ----------

# Diagnostics: real metadata key names we couldn't confirm from docs.
diag = {}
pk = ctx.stats.get("preflight", {}).get("po_metric_keys")
diag["po_metric_keys"] = pk.to_dict("records") if pk is not None else None
td = ctx.stats.get("uc", {}).get("table_detail")
opt_keys, ops_seen = {}, {}
if td is not None and not td.empty:
    sample = td.sort_values("num_files", ascending=False).head(15)
    for parts in sample["name_parts"]:
        try:
            for r in ctx.run_sql(f"DESCRIBE HISTORY {review.context.quote_ident(*parts)} LIMIT 500").collect():
                ops_seen[r["operation"]] = ops_seen.get(r["operation"], 0) + 1
                if r["operation"] in ("OPTIMIZE", "CLONE") or "CLONE" in r["operation"]:
                    for k, v in (r["operationParameters"] or {}).items():
                        opt_keys.setdefault(f"{r['operation']}.param.{k}", str(v)[:80])
                    for k, v in (r["operationMetrics"] or {}).items():
                        opt_keys.setdefault(f"{r['operation']}.metric.{k}", str(v)[:80])
        except Exception as exc:
            ops_seen.setdefault("ERR", str(exc)[:200])
    diag["table_detail_columns"] = list(td.columns)
    diag["table_detail_rows"] = len(td)
    diag["po_enabled_values"] = td["po_enabled"].astype(str).value_counts().to_dict() if "po_enabled" in td else None
    diag["po_setting_values"] = td["po_setting"].astype(str).value_counts().head(10).to_dict() if "po_setting" in td else None
diag["history_ops_seen"] = ops_seen
diag["optimize_clone_keys"] = opt_keys
tables = ctx.stats.get("uc", {}).get("tables")
if tables is not None:
    diag["table_types"] = tables["table_type"].astype(str).value_counts().to_dict()
    diag["formats"] = tables["data_source_format"].astype(str).value_counts().to_dict()

summary = {
    "timings_s": timings, "paths": paths, "sources": ctx.sources, "coverage_notes": ctx.coverage_notes,
    "stats_keys": {a: list(v) for a, v in ctx.stats.items()},
    "counts": ctx.stats.get("uc", {}).get("counts"), "storage": ctx.stats.get("uc", {}).get("storage_totals"),
    "hive_summary": ctx.stats.get("uc", {}).get("hive_summary"),
    "findings": [{"id": f.check_id, "sev": f.severity, "title": f.title, "n": len(f.evidence),
                  "fixes": len(f.remediation_sql)} for f in ctx.findings],
    "n_errors": len(ctx.errors), "errors_sample": ctx.errors[:25],
    "error_steps": {s: sum(e["step"] == s for e in ctx.errors) for s in {e["step"] for e in ctx.errors}},
    "diag": diag,
}
dbutils.notebook.exit(json.dumps(summary, default=str))
