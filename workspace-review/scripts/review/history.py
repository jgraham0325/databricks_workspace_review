"""Pure functions that summarise DESCRIBE HISTORY output. No Spark needed, so they're unit-tested locally.

Each history row is a dict with the DESCRIBE HISTORY columns; operationParameters and
operationMetrics are dicts of strings.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

CTAS_OPERATIONS = {"CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT"}
CLUSTERING_CHANGE_OPERATIONS = {"CLUSTER BY"}
EMPTY_PREDICATES = {"", "[]", "null", "None"}


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_table_name(name: Optional[str]) -> str:
    return (name or "").replace("`", "").strip().lower()


def is_hive_source(source: Optional[str]) -> bool:
    # Clones made from a path (delta.`s3://...`) don't say where they came from, so they count as non-Hive.
    return normalize_table_name(source).startswith("hive_metastore.")


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1")


def optimize_was_full(params: Dict[str, Any]) -> bool:
    """Was this OPTIMIZE run with FULL? The parameter name varies by runtime, so match any key containing 'full'."""
    return any("full" in k.lower() and _truthy(v) for k, v in (params or {}).items())


def summarize_history(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = sorted(rows, key=lambda r: r["version"])
    out: Dict[str, Any] = {
        "history_versions_read": len(rows),
        "oldest_version_read": rows[0]["version"] if rows else None,
        "migration_method": None,       # deep_clone | shallow_clone | ctas
        "migration_version": None,
        "migration_time": None,
        "clone_source": None,
        "clone_source_is_hive": None,
        "clone_files": None,            # files present right after the clone/CTAS
        "clone_bytes": None,
        "optimize_runs_after_migration": 0,
        "optimize_files_removed_after_migration": 0,
        "optimize_predicates": [],
        # Clustering keys were changed on a table that already had clustering keys, and no OPTIMIZE FULL has
        # run since. Plain OPTIMIZE only clusters data not yet clustered, so data clustered on the old keys stays
        # as it was. (Adding clustering to a never-clustered table doesn't need FULL: verified on DBR 15.4, a plain
        # OPTIMIZE rewrote all 3,000 cloned files.)
        "recluster_needed": False,
        "last_optimize": None,
        "last_vacuum": None,
    }

    for r in rows:
        op = (r.get("operation") or "").upper()
        params = r.get("operationParameters") or {}
        metrics = r.get("operationMetrics") or {}

        if "CLONE" in op:
            # Use the latest clone/CTAS in history: a re-run of the migration resets the baseline.
            out.update({
                "migration_method": "shallow_clone" if _truthy(params.get("isShallow")) else "deep_clone",
                "migration_version": r["version"],
                "migration_time": r.get("timestamp"),
                "clone_source": params.get("source"),
                "clone_source_is_hive": is_hive_source(params.get("source")),
                "clone_files": _int(metrics.get("sourceNumOfFiles")) or _int(metrics.get("numCopiedFiles")),
                "clone_bytes": _int(metrics.get("sourceTableSize")) or _int(metrics.get("copiedFilesSize")),
                "optimize_runs_after_migration": 0,
                "optimize_files_removed_after_migration": 0,
                "optimize_predicates": [],
            })
        elif op in CTAS_OPERATIONS:
            out.update({
                "migration_method": "ctas",
                "migration_version": r["version"],
                "migration_time": r.get("timestamp"),
                "clone_source": None,
                "clone_source_is_hive": None,
                "clone_files": _int(metrics.get("numFiles")),
                "clone_bytes": _int(metrics.get("numOutputBytes")),
                "optimize_runs_after_migration": 0,
                "optimize_files_removed_after_migration": 0,
                "optimize_predicates": [],
            })
        elif op == "OPTIMIZE":
            out["last_optimize"] = r.get("timestamp")
            out["optimize_runs_after_migration"] += 1
            out["optimize_files_removed_after_migration"] += _int(metrics.get("numRemovedFiles")) or 0
            pred = str(params.get("predicate", "")).strip()
            if pred not in EMPTY_PREDICATES:
                out["optimize_predicates"].append(pred)
            if optimize_was_full(params):
                out["recluster_needed"] = False
        elif op in CLUSTERING_CHANGE_OPERATIONS:
            old = str(params.get("oldClusteringColumns", "")).strip()
            if old not in EMPTY_PREDICATES:
                out["recluster_needed"] = True
        elif op.startswith("VACUUM"):
            out["last_vacuum"] = r.get("timestamp")

    # Distinct predicates only, newest last, capped for the report.
    out["optimize_predicates"] = list(dict.fromkeys(out["optimize_predicates"]))[-5:]
    return out


def rewrite_ratio(summary: Dict[str, Any], po_compacted_files: Optional[int] = None) -> Optional[float]:
    """Share of the migrated files that OPTIMIZE (manual or PO) has rewritten since the migration.

    Uses table history when the migration entry is still in it (PO compactions appear there too as
    OPTIMIZE), otherwise falls back to the PO system table total.
    """
    baseline = summary.get("clone_files")
    if not baseline:
        return None
    if summary.get("migration_version") is not None:
        rewritten = summary.get("optimize_files_removed_after_migration") or 0
    elif po_compacted_files is not None:
        rewritten = po_compacted_files
    else:
        return None
    return min(1.0, rewritten / baseline)
