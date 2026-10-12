"""Step 3: small-file backlogs, with special handling for tables migrated from hive_metastore.

A deep clone copies the Hive table's files as they are, so a fragmented Hive table becomes a fragmented
UC table. Ongoing maintenance (predictive optimization or scheduled OPTIMIZE) often leaves most of
those files untouched. This step finds those tables, estimates how much of the migrated data has been
rewritten, gives the most likely cause, and generates the commands to clear the backlog.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .checks import CheckSpec
from .context import Finding, ReviewContext, GB, SEVERITY_ORDER, evidence_records, plural, quote_ident
from .history import normalize_table_name, rewrite_ratio

AREA = "uc"

CAUSES = {
    "shallow_clone": "Shallow clone: the table still reads the Hive table's original small files.",
    "clustering_keys_changed": "Liquid clustering keys were changed after data was already clustered, and "
                               "OPTIMIZE FULL hasn't run since. Plain OPTIMIZE leaves data clustered on the old "
                               "keys as it is.",
    "over_partitioned": "Too many partitions with few files each. OPTIMIZE cannot merge files across partitions.",
    "scoped_optimize": "Scheduled OPTIMIZE runs with a WHERE filter, so older data is never compacted.",
    "po_disabled": "Predictive optimization is disabled for this table and no OPTIMIZE has cleared the backlog.",
    "po_not_running": "Predictive optimization is enabled but has not compacted this table.",
    "po_backlog": "Predictive optimization has compacted this table, but most of the migrated files remain. "
                  "PO compacts incrementally and has not caught up with a backlog this size.",
    "no_maintenance": "No OPTIMIZE has run on this table and predictive optimization status is unknown.",
    "unknown": "Cause unclear from the available metadata.",
}


def run(ctx: ReviewContext) -> Optional[pd.DataFrame]:
    result = prepare(ctx)
    evaluate(ctx)
    return result


def prepare(ctx: ReviewContext) -> Optional[pd.DataFrame]:
    stats = ctx.stats.get(AREA, {})
    hive_lineage = _lineage_map(stats.get("hive_lineage_targets"))
    df = stats.get("table_detail")
    if df is None or df.empty:
        ctx.add_stat(AREA, "small_file_tables", pd.DataFrame())
        return None
    cfg = ctx.config
    po = _po_map(stats.get("po_activity"))

    records = [clean_record(r) for r in df.to_dict("records")]
    rows = []
    for rec in records:
        if not is_small_files(rec, cfg):
            continue
        rec.update(provenance(rec, hive_lineage))
        rec.update(po.get(rec["full_name"].lower(), {"po_compaction_runs": 0, "po_compacted_files": None,
                                                     "po_clustering_runs": 0, "po_last_compaction": None}))
        rec["rewrite_ratio"] = rewrite_ratio(rec, rec["po_compacted_files"])
        rec["cause"] = classify_cause(rec, cfg)
        rec["severity"] = backlog_severity(rec, cfg)
        rows.append(rec)

    backlog = pd.DataFrame(rows)
    ctx.add_stat(AREA, "small_file_tables", backlog)

    if not backlog.empty:
        if cfg["deep_scan"]:
            _deep_scan(ctx, backlog)
    return backlog


def evaluate(ctx: ReviewContext) -> None:
    for check in CHECKS:
        check.evaluate(ctx)


def _run_backlog_check(ctx: ReviewContext, migrated: bool) -> None:
    backlog = ctx.stats.get(AREA, {}).get("small_file_tables")
    if backlog is None or backlog.empty:
        return
    rows = backlog[backlog["migrated"] == migrated]
    if not rows.empty:
        ctx.add_finding(_backlog_finding(ctx, rows, migrated=migrated))


def _run_hive_check(ctx: ReviewContext) -> None:
    stats = ctx.stats.get(AREA, {})
    detail = stats.get("table_detail")
    records = [] if detail is None else [clean_record(r) for r in detail.to_dict("records")]
    _check_hive_remaining(ctx, records, _lineage_map(stats.get("hive_lineage_targets")))


CHECKS = (
    CheckSpec("MIG-BACKLOG", AREA, "migration_backlog", lambda ctx: _run_backlog_check(ctx, True),
              ("small_file_avg_mb", "small_files_min_table_gb", "small_files_medium_count",
               "small_files_high_count", "migration_rewrite_ratio_high"), depends_on=("table_detail",)),
    CheckSpec("UC-SMALL-FILES", AREA, "migration_backlog", lambda ctx: _run_backlog_check(ctx, False),
              ("small_file_avg_mb", "small_files_min_table_gb", "small_files_medium_count",
               "small_files_high_count"), depends_on=("table_detail",)),
    CheckSpec("UC-HMS-REMAINING", AREA, "migration_backlog", _run_hive_check,
              depends_on=("uc_assets", "table_detail")),
)


# --- pure logic (unit tested) ----------------------------------------------------------

def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """pandas turns None into NaN and bools into numpy types after merges; undo that so the
    `is None` / `is True` checks below behave."""
    out = {}
    for k, v in rec.items():
        if isinstance(v, float) and pd.isna(v):
            v = None
        elif isinstance(v, np.bool_):
            v = bool(v)
        elif isinstance(v, np.integer):
            v = int(v)
        out[k] = v
    return out


def is_small_files(rec: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    avg = rec.get("avg_file_mb")
    return (rec.get("num_files", 0) >= cfg["small_files_medium_count"]
            and avg is not None and not pd.isna(avg) and avg < cfg["small_file_avg_mb"]
            and rec.get("size_bytes", 0) >= cfg["small_files_min_table_gb"] * GB)


def provenance(rec: Dict[str, Any], hive_lineage: Dict[str, str]) -> Dict[str, Any]:
    """Was this table migrated from hive_metastore, and how? Checks history, then UCX property, then lineage."""
    name = rec["full_name"].lower()
    props = rec.get("properties") or {}
    upgraded_from = props.get("upgraded_from")
    method = rec.get("migration_method")
    from_history = method in ("deep_clone", "shallow_clone") and rec.get("clone_source_is_hive")

    if from_history:
        label, source = method, rec.get("clone_source")
    elif upgraded_from:
        label, source = "ucx", upgraded_from
    elif name in hive_lineage:
        label, source = ("ctas" if method == "ctas" else "lineage"), hive_lineage[name]
    else:
        return {"migrated": False, "migration_label": None, "migration_source": None}
    return {"migrated": True, "migration_label": label, "migration_source": normalize_table_name(source)}


def classify_cause(rec: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    clustered = bool(rec.get("clustering_columns"))
    partitions = rec.get("partition_count")
    num_files = rec.get("num_files", 0)
    optimize_runs = _num(rec.get("optimize_runs_after_migration"))
    has_history = not _missing(rec.get("history_versions_read"))

    if rec.get("migration_label") == "shallow_clone":
        return "shallow_clone"
    if clustered and rec.get("recluster_needed") is True:
        return "clustering_keys_changed"
    if (not _missing(partitions) and partitions >= cfg["over_partitioned_min_partitions"]
            and num_files / max(partitions, 1) < cfg["over_partitioned_max_files_per_partition"]):
        return "over_partitioned"
    if rec.get("optimize_predicates"):
        return "scoped_optimize"
    if rec.get("po_enabled") is False:
        return "po_disabled"
    if rec.get("po_enabled") is True:
        po_runs = (rec.get("po_compaction_runs") or 0) + (rec.get("po_clustering_runs") or 0)
        return "po_backlog" if (po_runs or optimize_runs) else "po_not_running"
    if has_history and not optimize_runs:
        return "no_maintenance"
    return "unknown"


def backlog_severity(rec: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    if rec.get("num_files", 0) >= cfg["small_files_high_count"]:
        return "HIGH"
    if rec.get("migrated"):
        maintained = ((rec.get("po_compaction_runs") or 0) + (rec.get("po_clustering_runs") or 0) > 0
                      or _num(rec.get("optimize_runs_after_migration")) > 0)
        ratio = rec.get("rewrite_ratio")
        if maintained and ratio is not None and ratio < cfg["migration_rewrite_ratio_high"]:
            return "HIGH"
    return "MEDIUM"


def fix_command(rec: Dict[str, Any]) -> str:
    # FULL only when clustering keys changed: plain OPTIMIZE already compacts and clusters migrated files.
    ident = quote_ident(*rec["name_parts"])
    return f"OPTIMIZE {ident} FULL;" if rec.get("cause") == "clustering_keys_changed" else f"OPTIMIZE {ident};"


def remediation_sql(rec: Dict[str, Any]) -> str:
    header = (f"-- {rec['full_name']}: {int(rec['num_files']):,} files, {rec['size_bytes'] / GB:,.1f} GB "
              f"(avg {rec['avg_file_mb']} MB). Cause: {rec['cause']}.\n"
              f"-- Rewrites the whole table: run off-peak. Old files are removed by VACUUM after the retention "
              f"period (predictive optimization runs VACUUM automatically).\n")
    cmd = fix_command(rec)
    if rec["cause"] == "over_partitioned":
        header += ("-- Compacts within each partition only. Longer term, consider recreating the table with "
                   "liquid clustering instead of partitioning.\n")
    if rec["cause"] == "shallow_clone":
        header += "-- Keep the Hive source table until this completes: the clone still reads its files.\n"
    return header + cmd


def _num(value) -> int:
    return 0 if _missing(value) else int(value)


def _missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value))


# --- helpers -------------------------------------------------------------------------

def _lineage_map(lineage: Optional[pd.DataFrame]) -> Dict[str, str]:
    if lineage is None or lineage.empty:
        return {}
    return {r.target_table_full_name.lower(): r.hive_sources for r in lineage.itertuples()}


def _po_map(po: Optional[pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    if po is None or po.empty:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for r in po.itertuples():
        key = f"{r.catalog_name}.{r.schema_name}.{r.table_name}".lower()
        entry = out.setdefault(key, {"po_compaction_runs": 0, "po_compacted_files": None,
                                     "po_clustering_runs": 0, "po_last_compaction": None})
        op = (r.operation_type or "").upper()
        files = None if _missing(r.files_rewritten) else int(r.files_rewritten)
        if op == "COMPACTION":
            entry["po_compaction_runs"] += int(r.runs)
            entry["po_last_compaction"] = r.last_run
        elif op == "CLUSTERING":
            entry["po_clustering_runs"] += int(r.runs)
        else:
            continue
        # Both compaction and clustering rewrite the migrated files, so both count towards the backlog cleared.
        if files is not None:
            entry["po_compacted_files"] = (entry["po_compacted_files"] or 0) + files
    return out


def _backlog_finding(ctx: ReviewContext, rows: pd.DataFrame, migrated: bool) -> Finding:
    rows = rows.sort_values("num_files", ascending=False)
    severity = min(rows["severity"], key=SEVERITY_ORDER.get)
    total_files = int(rows["num_files"].sum())
    causes = rows["cause"].value_counts().to_dict()

    ev = pd.DataFrame({
        "table": rows["full_name"],
        "files": rows["num_files"].astype("int64"),
        "avg_file_mb": rows["avg_file_mb"],
        "size_gb": (rows["size_bytes"] / GB).round(2),
        "po_enabled": rows.get("po_enabled"),
        "po_compactions": rows["po_compaction_runs"],
        "severity": rows["severity"],
        "cause": rows["cause"],
    })
    if migrated:
        ev.insert(1, "migrated_via", rows["migration_label"])
        ev.insert(2, "source", rows["migration_source"])
        ev.insert(4, "files_at_migration", rows.get("clone_files"))
        ev["rewritten_pct"] = rows["rewrite_ratio"].map(lambda r: None if _missing(r) else round(100 * r, 1))
    ev["fix_command"] = [fix_command(r) for r in rows.to_dict("records")]

    cause_text = "\n".join(f"- **{c}** ({n} tables): {CAUSES[c]}" for c, n in causes.items())
    if migrated:
        return Finding(
            check_id="MIG-BACKLOG",
            area=AREA,
            severity=severity,
            title=f"{plural(len(rows), 'table')} migrated from hive_metastore with {total_files:,} small files left",
            summary="These tables were migrated with their original small files, and ongoing maintenance has "
                    "not compacted most of them.\n\nLikely causes:\n" + cause_text,
            impact="Every query on these tables opens many more files than needed: slower queries, higher "
                   "compute cost, and higher cloud storage API costs. Predictive optimization alone is unlikely "
                   "to clear a backlog this size.",
            recommendation="Run a one-time OPTIMIZE on each table, largest first, off-peak (OPTIMIZE ... FULL only "
                           "where clustering keys were changed). Commands are in the remediation file.",
            evidence=evidence_records(ev),
            doc_url="https://docs.databricks.com/delta/clustering.html",
            remediation_sql=[remediation_sql(r) for r in rows.to_dict("records")],
        )
    return Finding(
        check_id="UC-SMALL-FILES",
        area=AREA,
        severity=severity,
        title=f"{plural(len(rows), 'table')} with a small-file problem ({total_files:,} files in total)",
        summary=f"Tables of at least {ctx.config['small_files_min_table_gb']} GB with "
                f"{ctx.config['small_files_medium_count']:,}+ files averaging under "
                f"{ctx.config['small_file_avg_mb']} MB.\n\nLikely causes:\n" + cause_text,
        impact="Slower queries and higher compute and storage API costs.",
        recommendation="Run a one-time OPTIMIZE (FULL only where clustering keys were changed) and keep "
                       "predictive optimization enabled to stop the backlog building up again.",
        evidence=evidence_records(ev),
        doc_url="https://docs.databricks.com/delta/optimize.html",
        remediation_sql=[remediation_sql(r) for r in rows.to_dict("records")],
    )


def _check_hive_remaining(ctx: ReviewContext, records: List[Dict[str, Any]],
                          hive_lineage: Dict[str, str]) -> None:
    hive = ctx.stats.get(AREA, {}).get("hive_tables")
    if hive is None or hive.empty:
        return
    # hive source table -> UC copies found (via lineage, clone history, or the UCX property).
    copies: Dict[str, set] = {}
    for target, sources in hive_lineage.items():
        for s in sources.split(","):
            copies.setdefault(normalize_table_name(s), set()).add(target)
    for rec in records:
        if rec.get("clone_source_is_hive") is True:
            copies.setdefault(normalize_table_name(rec.get("clone_source")), set()).add(rec["full_name"])
        upgraded = (rec.get("properties") or {}).get("upgraded_from")
        if upgraded:
            copies.setdefault(normalize_table_name(upgraded), set()).add(rec["full_name"])

    hive = hive.copy()
    hive["uc_copy"] = hive["full_name"].str.lower().map(lambda n: ", ".join(sorted(copies.get(n, ()))) or None)
    not_migrated = hive[hive["uc_copy"].isna()]
    ctx.add_stat(AREA, "hive_summary", {
        "hive_tables": len(hive),
        "with_known_uc_copy": len(hive) - len(not_migrated),
        "without_known_uc_copy": len(not_migrated),
    })
    per_schema = hive.groupby("schema").size().sort_values(ascending=False)
    top = ", ".join(f"`{s}` ({n})" for s, n in per_schema.head(5).items())
    more = f", and {len(per_schema) - 5} more schemas" if len(per_schema) > 5 else ""
    evidence = hive.sort_values(["uc_copy", "schema", "table"], na_position="first")
    ctx.add_finding(Finding(
        check_id="UC-HMS-REMAINING",
        area=AREA,
        severity="MEDIUM" if len(not_migrated) else "LOW",
        title=f"{plural(len(hive), 'table')} left in hive_metastore "
              f"({len(not_migrated)} with no known Unity Catalog copy)",
        summary=f"Tables still registered in the legacy Hive metastore, across {plural(len(per_schema), 'schema')}: "
                f"{top}{more}. A UC copy is 'known' when it was found via clone history, the UCX `upgraded_from` "
                "property, or lineage, so the real number of migrated tables may be higher.",
        impact="Hive metastore tables are outside Unity Catalog governance, lineage, and auditing, and can't use "
               "predictive optimization. Leftover copies confuse users about which table is current.",
        recommendation="Migrate the remaining tables (UCX or DEEP CLONE). For tables already migrated, confirm "
                       "nothing still writes to the Hive copy, then retire it.",
        evidence=evidence[["full_name", "schema", "uc_copy"]].astype(object)
            .where(evidence[["full_name", "schema", "uc_copy"]].notna(), None).to_dict("records"),
    ))



def _deep_scan(ctx: ReviewContext, backlog: pd.DataFrame) -> None:
    """Optional: actual file-size distribution for the largest HIGH tables. Reads every file (full scan)."""
    targets = backlog[backlog["severity"] == "HIGH"].sort_values("num_files", ascending=False)
    targets = targets.head(ctx.config["deep_scan_max_tables"])
    results = []
    for rec in targets.to_dict("records"):
        sql = f"""
            SELECT CASE WHEN file_size < 1048576 THEN '1: < 1 MB'
                        WHEN file_size < 8388608 THEN '2: 1-8 MB'
                        WHEN file_size < 33554432 THEN '3: 8-32 MB'
                        WHEN file_size < 134217728 THEN '4: 32-128 MB'
                        ELSE '5: >= 128 MB' END AS size_bucket,
                   count(*) AS files, round(sum(file_size) / 1073741824, 2) AS gb
            FROM (SELECT DISTINCT _metadata.file_path AS file_path, _metadata.file_size AS file_size
                  FROM {quote_ident(*rec['name_parts'])})
            GROUP BY 1 ORDER BY 1"""
        try:
            pdf = ctx.query_pdf(sql)
            pdf.insert(0, "table", rec["full_name"])
            results.append(pdf)
        except Exception as exc:
            ctx.record_error(rec["full_name"], "deep scan", exc)
    if results:
        ctx.add_stat(AREA, "file_size_distribution", pd.concat(results, ignore_index=True))
