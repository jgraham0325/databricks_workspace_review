# Databricks workspace review skill

A Databricks Assistant (agent mode) skill that runs a read-only review of the current workspace and reports
stats, problems, and remediation commands. The skill itself is in `workspace-review/`.

## Repository layout

- `workspace-review/`: the deployable Assistant skill.
- `tests/`: local tests for logic that does not require Spark.
- `dev/`: parameterized notebooks for live testing; these are not deployed with the skill.

## Output (one folder per run)

`/Workspace/Users/<user>/workspace_reviews/<YYYY-MM-DD_HHMM>/`: `report.md` (fixed structure from
`assets/report_template.md`), `drilldown` notebook (every affected object per finding), `remediation.sql`,
and `data/*.csv`.

## Install into a workspace

Personal install (only you see it):

```bash
databricks workspace import-dir workspace-review \
  "/Workspace/Users/<you>/.assistant/skills/workspace-review" --overwrite --profile <profile>
```

Workspace-wide (admin): import into `/Workspace/.assistant/skills/workspace-review` instead.
Then, in a notebook with the Assistant in agent mode, ask: "run a workspace review".

## Live testing (`dev/`)

Not part of the skill. Import each into a workspace as a notebook, then run it as a one-time job:

- `run_review_test.py`: runs the review exactly as `SKILL.md` does, with per-step timings and metadata diagnostics.
  Job parameters: `skill_dir`, `include` (comma-separated catalogs), `schemas` (comma-separated schema or
  catalog.schema names), and `overrides` (JSON).
- `seed_migration_scenario.py`: creates a Hive → UC deep-clone scenario with small files in
  `hive_metastore.wr_seed` and `<uc_catalog>.wr_seed`. Set the required `uc_catalog` job parameter. It writes to
  `hive_metastore`, so use compatible classic compute. Review it with
  `{"small_files_min_table_gb": 0, "small_files_medium_count": 200,
  "small_files_high_count": 2500, "history_min_files": 100}`.
- `drop_seed.py`: drops only the seed's objects after checking the schema marker. Pass the same `uc_catalog`.

## Local tests

Pure logic (history parsing, cause classification, severity, the read-only SQL guard) runs without Spark:

```bash
python3 -m pip install -e '.[test]'
python3 -m pytest
```

Python 3.9 or newer is supported. The skill is imported directly from `workspace-review/scripts`; it is not a
pip package and `workspace-review/` remains the unit deployed to Databricks.

## Roadmap

- Add workspace-level cost trends and grants/governance checks.
- Run and document the parameterized seeded scenario in a workspace.

Add a license before publishing this repository publicly. A private repository does not require one.
