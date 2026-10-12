"""Step 2: per-table metadata (DESCRIBE DETAIL / HISTORY / EXTENDED) and table-maintenance checks."""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from .checks import CheckSpec
from .context import Finding, ReviewContext, GB, plural, quote_ident
from .history import summarize_history

AREA = "uc"


def run(ctx: ReviewContext) -> Optional[pd.DataFrame]:
    result = collect(ctx)
    evaluate(ctx)
    return result


def collect(ctx: ReviewContext) -> Optional[pd.DataFrame]:
    tables = ctx.stats.get(AREA, {}).get("tables")
    if tables is None or tables.empty:
        return None

    delta = tables[tables["table_type"].isin(["MANAGED", "EXTERNAL"])
                   & (tables["data_source_format"].fillna("").str.upper() == "DELTA")]
    cap = ctx.config["max_tables_detail"]
    if len(delta) > cap:
        ctx.note(f"{len(delta)} Delta tables found; per-table detail limited to the {cap} most recently altered.")
        delta = delta.sort_values("last_altered", ascending=False).head(cap)

    names = [(r.table_catalog, r.table_schema, r.table_name) for r in delta.itertuples()]
    detail = _parallel(ctx, names, lambda n: _describe_detail(ctx, n), "DESCRIBE DETAIL")
    if not detail:
        return None
    df = pd.DataFrame(detail)
    df["avg_file_mb"] = (df["size_bytes"] / df["num_files"].where(df["num_files"] > 0) / 1024 ** 2).round(2)

    # History, PO status, and partition counts only for tables big enough to need maintenance.
    deep = df[df["num_files"] >= ctx.config["history_min_files"]]
    partitioned = set(deep.loc[deep["partition_columns"].map(bool), "full_name"])
    extra = _parallel(ctx, list(deep["name_parts"]),
                      lambda n: _describe_deep(ctx, n, ".".join(n) in partitioned), "DESCRIBE HISTORY/EXTENDED")
    if extra:
        df = df.merge(pd.DataFrame(extra), on="full_name", how="left")

    ctx.add_stat(AREA, "table_detail", df)
    ctx.add_stat(AREA, "storage_totals", {
        "tables_inspected": len(df),
        "total_size_gb": round(df["size_bytes"].sum() / GB, 1),
        "total_files": int(df["num_files"].sum()),
        "clustered_tables": int(df["clustering_columns"].map(bool).sum()),
        "partitioned_tables": int(df["partition_columns"].map(bool).sum()),
    })
    ctx.add_stat(AREA, "largest_tables", _top(df, "size_bytes"))
    ctx.add_stat(AREA, "most_files", _top(df, "num_files"))

    return df


def evaluate(ctx: ReviewContext) -> None:
    for check in CHECKS:
        check.evaluate(ctx)


def _run_check(ctx: ReviewContext, fn) -> None:
    detail = ctx.stats.get(AREA, {}).get("table_detail")
    if detail is not None and not detail.empty:
        fn(ctx, detail)


CHECKS = (
    CheckSpec("UC-NO-MAINTENANCE", AREA, "table_detail",
              lambda ctx: _run_check(ctx, _check_no_maintenance),
              ("activity_window_days", "history_min_files"), depends_on=("uc_assets",)),
    CheckSpec("UC-NO-LAYOUT", AREA, "table_detail", lambda ctx: _run_check(ctx, _check_no_layout),
              ("no_layout_min_table_gb",), depends_on=("uc_assets",)),
)


# --- collection --------------------------------------------------------------------

def _parallel(ctx: ReviewContext, items: List[tuple], fn: Callable, step: str) -> List[Dict[str, Any]]:
    def safe(item):
        try:
            return fn(item)
        except Exception as exc:
            ctx.record_error(".".join(item), step, exc)
            return None

    with ThreadPoolExecutor(max_workers=ctx.config["detail_parallelism"]) as pool:
        return [r for r in pool.map(safe, items) if r]


def _describe_detail(ctx: ReviewContext, name: tuple) -> Dict[str, Any]:
    d = ctx.run_sql(f"DESCRIBE DETAIL {quote_ident(*name)}").first().asDict()
    return {
        "full_name": ".".join(name),
        "name_parts": tuple(name),
        "num_files": int(d.get("numFiles") or 0),
        "size_bytes": int(d.get("sizeInBytes") or 0),
        "partition_columns": list(d.get("partitionColumns") or []),
        "clustering_columns": list(d.get("clusteringColumns") or []),
        "properties": dict(d.get("properties") or {}),
        "last_modified": d.get("lastModified"),
    }


def _describe_deep(ctx: ReviewContext, name: tuple, is_partitioned: bool) -> Dict[str, Any]:
    ident = quote_ident(*name)
    out: Dict[str, Any] = {"full_name": ".".join(name)}

    rows = ctx.run_sql(f"DESCRIBE HISTORY {ident} LIMIT {int(ctx.config['history_limit'])}").collect()
    out.update(summarize_history(r.asDict() for r in rows))

    extended = ctx.run_sql(f"DESCRIBE TABLE EXTENDED {ident}").collect()
    out.update(parse_po_status(extended))

    out["partition_count"] = ctx.run_sql(f"SHOW PARTITIONS {ident}").count() if is_partitioned else None
    return out


def parse_po_status(extended_rows) -> Dict[str, Any]:
    """Read 'Predictive Optimization' from DESCRIBE TABLE EXTENDED, e.g. 'ENABLE (inherited from CATALOG main)'."""
    for r in extended_rows:
        if (r[0] or "").strip().lower() == "predictive optimization":
            value = (r[1] or "").strip()
            m = re.search(r"\((.*)\)", value)
            return {"po_enabled": value.upper().startswith("ENABLE"),
                    "po_setting": value,
                    "po_inherited_from": m.group(1) if m else None}
    return {"po_enabled": None, "po_setting": None, "po_inherited_from": None}


def _top(df: pd.DataFrame, col: str, n: int = 20) -> pd.DataFrame:
    cols = ["full_name", "size_bytes", "num_files", "avg_file_mb", "partition_columns", "clustering_columns"]
    top = df.sort_values(col, ascending=False).head(n)[cols].copy()
    top["size_gb"] = (top.pop("size_bytes") / GB).round(1)
    return top


# --- checks --------------------------------------------------------------------

def _recent(ts, days: int) -> bool:
    if ts is None or pd.isna(ts):
        return False
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts >= pd.Timestamp(datetime.now(timezone.utc) - timedelta(days=days))


def _check_no_maintenance(ctx: ReviewContext, df: pd.DataFrame) -> None:
    if "po_enabled" not in df:
        return
    days = ctx.config["activity_window_days"]
    deep = df[df["history_versions_read"].notna()].copy()
    deep["optimized_recently"] = deep["last_optimize"].map(lambda t: _recent(t, days))
    deep["vacuumed_recently"] = deep["last_vacuum"].map(lambda t: _recent(t, days))
    stale = deep[(deep["po_enabled"] != True)  # noqa: E712 (None counts as not enabled)
                 & ~(deep["optimized_recently"] & deep["vacuumed_recently"])]
    if stale.empty:
        return
    ev = stale[["full_name", "num_files", "avg_file_mb", "po_setting", "last_optimize", "last_vacuum"]].copy()
    ev["size_gb"] = (stale["size_bytes"] / GB).round(1)
    ctx.add_finding(Finding(
        check_id="UC-NO-MAINTENANCE",
        area=AREA,
        severity="MEDIUM",
        title=f"{plural(len(stale), 'large table')} without predictive optimization or recent OPTIMIZE/VACUUM",
        summary=f"Tables with at least {ctx.config['history_min_files']:,} files, predictive optimization not "
                f"enabled, and no OPTIMIZE and VACUUM in the last {days} days.",
        impact="Files keep fragmenting, queries slow down, and storage holds old files that are no longer needed.",
        recommendation="Enable predictive optimization at catalog or schema level "
                       "(ALTER CATALOG <c> ENABLE PREDICTIVE OPTIMIZATION), or schedule OPTIMIZE and VACUUM.",
        evidence=ev.sort_values("num_files", ascending=False).astype(str).to_dict("records"),
        doc_url="https://docs.databricks.com/optimizations/predictive-optimization.html",
    ))


def _check_no_layout(ctx: ReviewContext, df: pd.DataFrame) -> None:
    big = df[(df["size_bytes"] >= ctx.config["no_layout_min_table_gb"] * GB)
             & ~df["partition_columns"].map(bool) & ~df["clustering_columns"].map(bool)]
    if big.empty:
        return
    ev = big[["full_name", "num_files"]].copy()
    ev["size_gb"] = (big["size_bytes"] / GB).round(1)
    ctx.add_finding(Finding(
        check_id="UC-NO-LAYOUT",
        area=AREA,
        severity="LOW",
        title=f"{plural(len(big), 'table')} over {ctx.config['no_layout_min_table_gb']} GB with no clustering or partitioning",
        summary="Large tables with no data layout rely only on file statistics for data skipping.",
        impact="Selective queries may read far more data than needed.",
        recommendation="Use liquid clustering on the common filter columns, or CLUSTER BY AUTO to let "
                       "predictive optimization choose them (requires predictive optimization). Review each "
                       "table's query patterns first; this changes the table layout.",
        evidence=ev.sort_values("size_gb", ascending=False).to_dict("records"),
        doc_url="https://docs.databricks.com/delta/clustering.html",
        remediation_sql=[f"-- REVIEW FIRST: changes data layout\nALTER TABLE {quote_ident(*parts)} CLUSTER BY AUTO;"
                         for parts in big.sort_values("size_bytes", ascending=False)["name_parts"]],
    ))
