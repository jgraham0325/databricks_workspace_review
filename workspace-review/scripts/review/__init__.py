"""Databricks workspace review: read-only collection and checks, driven by the workspace-review skill."""

from .config import REVIEW_AREAS
from .context import Finding, ReviewContext  # noqa: F401
from . import preflight, uc_assets, table_detail, migration_backlog, jobs, compute, settings  # noqa: F401
from .checks import REGISTERED_CHECKS, register_checks, run_checks

REGISTERED_CHECKS.clear()
for _area_checks in (
    uc_assets.CHECKS,
    table_detail.CHECKS,
    migration_backlog.CHECKS,
    jobs.CHECKS,
    compute.CHECKS,
    settings.CHECKS,
):
    register_checks(_area_checks)

from . import report  # noqa: E402,F401
from .report import write_report  # noqa: F401


def _uc_assets(ctx):
    uc_assets.collect(ctx)
    run_checks(ctx, REGISTERED_CHECKS, "uc_assets")


def _table_detail(ctx):
    table_detail.collect(ctx)
    run_checks(ctx, REGISTERED_CHECKS, "table_detail")


def _migration_backlog(ctx):
    migration_backlog.prepare(ctx)
    run_checks(ctx, REGISTERED_CHECKS, "migration_backlog")


def _jobs(ctx):
    if ctx.workspace_id is None:
        ctx.note("Jobs section skipped: workspace ID unknown, so system tables can't be filtered to this workspace.")
        return
    jobs.collect(ctx)
    jobs.prepare(ctx)
    run_checks(ctx, REGISTERED_CHECKS, "jobs")


def _compute(ctx):
    if ctx.workspace_id is None:
        ctx.note("Compute section skipped: workspace ID unknown, so system tables can't be filtered to this workspace.")
        return
    compute.collect(ctx)
    compute.prepare(ctx)
    run_checks(ctx, REGISTERED_CHECKS, "compute")


def _settings(ctx):
    ctx.add_stat("settings", "raw", settings.collect(ctx))
    settings.prepare(ctx)
    run_checks(ctx, REGISTERED_CHECKS, "settings")


# Collection stages run in this order. jobs precedes compute because CMP-RUNTIME also checks job definitions.
STEPS = [
    ("preflight", preflight.run),
    ("uc_assets", _uc_assets),
    ("table_detail", _table_detail),
    ("migration_backlog", _migration_backlog),
    ("jobs", _jobs),
    ("compute", _compute),
    ("settings", _settings),
]

# Which review area a step belongs to. preflight always runs.
STEP_AREA = {
    "preflight": None,
    "uc_assets": "uc",
    "table_detail": "uc",
    "migration_backlog": "uc",
    "jobs": "jobs",
    "compute": "compute",
    "settings": "settings",
}


def steps_to_run(ctx: ReviewContext):
    """STEPS entries this run will execute, honoring include_areas."""
    return [(name, step) for name, step in STEPS
            if STEP_AREA[name] is None or ctx.area_in_scope(STEP_AREA[name])]


def run_all(ctx: ReviewContext, verbose: bool = True, output_dir: str = None) -> dict:
    """Run every in-scope step, then write the report folder (report, remediation SQL, drill-down notebook, data).
    Returns the paths.

    A failing step is recorded and the review continues with the next one, so a report is always written.
    """
    skipped = [area for area in REVIEW_AREAS if not ctx.area_in_scope(area)]
    if skipped:
        ctx.note("Skipped " + ", ".join(skipped) + " (include_areas="
                 + ", ".join(ctx.included_areas()) + ").")
    for name, step in steps_to_run(ctx):
        if verbose:
            print(f"[workspace-review] {name} ...", flush=True)
        try:
            step(ctx)
        except Exception as exc:
            ctx.record_error(name, "step", exc)
            ctx.note(f"Step `{name}` failed and its section is incomplete: {str(exc).splitlines()[0][:200]}")
    paths = write_report(ctx, output_dir)
    if verbose:
        print(f"[workspace-review] done: {len(ctx.findings)} findings, {len(ctx.errors)} object errors", flush=True)
        for label in ("folder", "report", "notebook", "remediation_sql"):
            print(f"[workspace-review] {label + ':':17} {paths[label]}", flush=True)
    return paths


def findings_table(ctx: ReviewContext):
    """Findings as a pandas DataFrame, most severe first (for display in the notebook)."""
    import pandas as pd
    from .context import SEVERITY_ORDER
    rows = [{"severity": f.severity, "area": f.area, "check_id": f.check_id, "title": f.title,
             "objects": len(f.evidence)} for f in ctx.findings]
    df = pd.DataFrame(rows, columns=["severity", "area", "check_id", "title", "objects"])
    return df.sort_values("severity", key=lambda s: s.map(SEVERITY_ORDER)).reset_index(drop=True)
