---
name: workspace-review
description: Use when asked to review, audit, or health-check this Databricks workspace — its catalogs, schemas, tables, small-file problems, Hive-to-Unity-Catalog migration leftovers, jobs and pipelines, clusters and SQL warehouses, and workspace security settings. Runs read-only queries, flags problems with severities, and saves a report, a drill-down notebook listing every affected table, and copy-paste remediation SQL to the user's workspace folder.
---

# Workspace review

Run a **read-only** review of the current workspace. The collection, checks, report, and drill-down notebook
are all produced by the Python in `scripts/review/`. Your job is to confirm settings, run it, and point the
user to the outputs.

## Rules

- **The code writes the report. You don't.** Never write, rewrite, restructure, or reformat a review report
  yourself, in chat or in a file. The report structure comes from `assets/report_template.md` and is filled in
  by `review.run_all`. If the user wants the structure changed, the template or `scripts/review/report.py`
  must be changed.
- **Never change anything in the workspace.** Do not run OPTIMIZE, ALTER, VACUUM, GRANT, DROP, or any other
  write, even if a finding recommends it or the user asks you to "fix it" during the review. Fix commands are
  saved for a human to review and run. The code enforces this (`ctx.run_sql` refuses anything but SELECT,
  WITH, DESCRIBE, SHOW). The only thing written is the output folder under
  `/Workspace/Users/<user>/workspace_reviews/`.
- **Costs are list price.** When you mention a cost, say it's at list price, not the customer's contracted price.
- **Don't make up numbers.** Anything you say about results must come from the generated report, the
  drill-down data, `ctx.stats`, or `ctx.findings`.

## Steps

### 1. Confirm settings with the user
Ask, briefly, before running (use defaults if the user says "just run it"):
- **Which catalogs** to review: all (default), or a specific list (`include_catalogs`). This only scopes the
  Unity Catalog section; jobs, compute, and settings always cover the whole workspace. Add
  `hive_metastore` to the list to also inventory legacy Hive tables; it's included automatically only when
  reviewing all catalogs.
- **Catalogs to skip**, beyond `system`, `samples`, `__databricks_internal` (e.g. Delta Sharing or foreign
  catalogs). Applied on top of the include list.
- **Activity window** in days (default **30**). Explain it as: how far back to look at activity: job runs and
  failures, cluster utilization, spend, and whether OPTIMIZE and VACUUM have run recently. It does **not** limit which
  tables are reviewed: tables of any age are included, and migration history is always read in full.
- **Deep scan** of the worst small-file tables (default **off**; it reads every file of up to 10 tables, so it
  costs compute).

### 2. Run the review in a notebook (serverless or DBR 15.4+)
Set `SKILL_DIR` to the folder containing this SKILL.md, usually
`/Workspace/Users/<user>/.assistant/skills/workspace-review` or `/Workspace/.assistant/skills/workspace-review`.
Run this as one cell, changing only the settings from step 1:

```python
import sys
SKILL_DIR = "/Workspace/Users/<user>/.assistant/skills/workspace-review"
sys.path.insert(0, f"{SKILL_DIR}/scripts")
for m in [m for m in sys.modules if m == "review" or m.startswith("review.")]:
    del sys.modules[m]  # pick up any updated skill files
import review

ctx = review.ReviewContext.create(
    spark, SKILL_DIR,
    include_catalogs=[],          # e.g. ["sales", "finance"]; empty = all catalogs
    activity_window_days=30,
    overrides={"extra_excluded_catalogs": [], "deep_scan": False},
)
paths = review.run_all(ctx)
display(review.findings_table(ctx))
```

A run on a few thousand tables takes several minutes, mostly per-table `DESCRIBE` commands. Workspace settings
need a workspace admin; for anyone else they show as unknown. If a step fails,
`run_all` records it and carries on, and the report is still written, listing the gap under Coverage.

`run_all` writes one folder per run, `/Workspace/Users/<user>/workspace_reviews/<YYYY-MM-DD_HHMM>/`:

| Output | What it is |
|---|---|
| `report.md` | Summary report: scores, top findings, overview, findings by area, migration health, appendix |
| `drilldown` | Notebook with one section per finding listing **every** affected object and its fix command |
| `remediation.sql` | Every fix command, grouped by finding, for review in the SQL editor |
| `data/*.csv` | Full evidence per finding (what the notebook reads) |

### 3. Tell the user where the outputs are
Reply in chat with only:
1. The paths from `paths` (`folder`, `report`, `notebook`, `remediation_sql`), the report and notebook first.
2. The executive summary, copied **verbatim** from `report.md`: read the file and paste the text from
   `## Executive summary` up to, but not including, `## Workspace overview`. Don't reword, reorder, or add to it.
3. Any coverage gaps: `ctx.coverage_notes`, and the count of `ctx.errors` if it's not zero. If
   `system.information_schema` wasn't readable, say which grants are needed (see `references/system_tables.md`).
4. One line: "Open the drill-down notebook and click **Run all** to see every affected table."

### 4. Follow-up questions
To answer questions about specific findings, read the CSV files in `data/`, or `ctx.findings` and
`ctx.stats` while the notebook session is still live. Use `references/checks.md` to explain what a check
or cause means. Don't re-run the review unless the user asks.
