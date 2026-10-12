"""Local tests for the parts of the review that don't need Spark. Run: python -m pytest tests"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_DIR = os.path.join(ROOT, "workspace-review")

from review import migration_backlog as mb  # noqa: E402
from review.config import DEFAULTS  # noqa: E402
from review.context import (Finding, NotReadOnlyError, ReviewContext, assert_read_only,  # noqa: E402
                            load_named_queries, plural, principal_kind, quote_ident)
from review.history import optimize_was_full, rewrite_ratio, summarize_history  # noqa: E402
from review.table_detail import parse_po_status  # noqa: E402
from review.uc_assets import CATALOG_COLUMNS  # noqa: E402

GB = 1024 ** 3


# --- SQL safety and helpers ----------------------------------------------------------------

@pytest.mark.parametrize("stmt", [
    "SELECT 1", "  with x as (select 1) select * from x", "DESCRIBE DETAIL `a`.`b`.`c`",
    "-- comment\nSHOW PARTITIONS t", "DESCRIBE HISTORY t LIMIT 10;",
])
def test_read_only_allows(stmt):
    assert_read_only(stmt)


@pytest.mark.parametrize("stmt", [
    "OPTIMIZE t", "DROP TABLE t", "ALTER TABLE t CLUSTER BY AUTO", "/* SELECT */ DELETE FROM t",
    "SELECT 1; DROP TABLE t", "CREATE TABLE t AS SELECT 1",
])
def test_read_only_blocks(stmt):
    with pytest.raises(NotReadOnlyError):
        assert_read_only(stmt)


def test_quote_ident_escapes_backticks():
    assert quote_ident("main", "we`ird", "t") == "`main`.`we``ird`.`t`"


@pytest.mark.parametrize("name,kind", [
    ("jane@acme.com", "user"), ("data-engineers", "group"), (None, "none"),
    ("0a1b2c3d-1111-2222-3333-444455556666", "service_principal"),
])
def test_principal_kind(name, kind):
    assert principal_kind(name) == kind


@pytest.mark.parametrize("filename,expected", [
    ("preflight.sql", {"information_schema", "billing", "predictive_optimization", "hive_metastore"}),
    ("uc_assets.sql", {"catalogs", "schemas", "tables", "volumes", "hive_lineage_targets", "po_activity"}),
    ("jobs.sql", {"jobs_current", "job_runs", "job_task_compute", "pipeline_updates", "job_cost"}),
    ("compute.sql", {"clusters_current", "cluster_utilization", "compute_cost", "warehouses_current"}),
])
def test_sql_files_parse_render_and_are_read_only(filename, expected):
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR, include_catalogs=["main"])
    ctx.workspace_id = 123
    queries = load_named_queries(os.path.join(SKILL_DIR, "scripts", "sql", filename))
    assert expected <= set(queries)
    for name, sql in queries.items():
        rendered = ctx.render(sql, catalog_col=CATALOG_COLUMNS.get(name))
        assert "{" not in rendered
        assert_read_only(rendered)
        if "{catalog_filter}" in sql:
            assert "IN ('main')" in rendered


def test_every_filtered_query_has_a_catalog_column():
    queries = load_named_queries(os.path.join(SKILL_DIR, "scripts", "sql", "uc_assets.sql"))
    assert {n for n, sql in queries.items() if "{catalog_filter}" in sql} == set(CATALOG_COLUMNS)


def test_catalog_filter_exclude_only():
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR, overrides={"extra_excluded_catalogs": ["Shared"]})
    sql = ctx.catalog_filter_sql("table_catalog")
    assert sql == "lower(table_catalog) NOT IN ('system', 'samples', '__databricks_internal', 'shared')"
    assert ctx.catalog_in_scope("main") and ctx.catalog_in_scope("hive_metastore")
    assert not ctx.catalog_in_scope("SHARED")


def test_catalog_filter_include_list():
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR, include_catalogs=["Sales", "finance"])
    assert ctx.catalog_filter_sql("c").endswith("AND lower(c) IN ('sales', 'finance')")
    assert ctx.catalog_in_scope("sales")
    assert not ctx.catalog_in_scope("main")
    assert not ctx.catalog_in_scope("hive_metastore")  # only when listed explicitly
    ctx2 = ReviewContext.create(spark=None, skill_dir=SKILL_DIR, include_catalogs=["sales", "hive_metastore"])
    assert ctx2.catalog_in_scope("hive_metastore")


def test_unknown_setting_rejected():
    with pytest.raises(ValueError, match="lookback_days"):
        ReviewContext.create(spark=None, skill_dir=SKILL_DIR, overrides={"lookback_days": 7})


def test_finding_rejects_unknown_severity():
    with pytest.raises(ValueError):
        Finding("X", "uc", "CRITICAL", "t", "s", "i", "r")


# --- history parsing --------------------------------------------------------------------

def _h(version, operation, params=None, metrics=None):
    return {"version": version, "timestamp": datetime(2026, 1, 1) + timedelta(days=version), "operation": operation,
            "operationParameters": params or {}, "operationMetrics": metrics or {}}


def test_deep_clone_then_partial_po_optimize():
    rows = [
        _h(0, "CLONE", {"source": "`hive_metastore`.`sales`.`orders`", "isShallow": "false"},
           {"sourceNumOfFiles": "150000", "sourceTableSize": str(500 * GB)}),
        _h(1, "WRITE"),
        _h(2, "OPTIMIZE", {"predicate": "[]", "auto": "true"}, {"numRemovedFiles": "3000"}),
        _h(3, "OPTIMIZE", {"predicate": "[]", "auto": "true"}, {"numRemovedFiles": "2000"}),
        _h(4, "VACUUM END"),
    ]
    s = summarize_history(reversed(rows))  # DESCRIBE HISTORY returns newest first
    assert s["migration_method"] == "deep_clone"
    assert s["clone_source_is_hive"] is True
    assert s["clone_files"] == 150000
    assert s["optimize_runs_after_migration"] == 2
    assert s["optimize_files_removed_after_migration"] == 5000
    assert s["optimize_predicates"] == []
    assert s["recluster_needed"] is False
    assert s["last_vacuum"] == datetime(2026, 1, 5)
    assert rewrite_ratio(s) == pytest.approx(5000 / 150000)


def test_shallow_clone_and_scoped_optimize():
    rows = [
        _h(0, "CLONE", {"source": "hive_metastore.a.b", "isShallow": "true"}, {"sourceNumOfFiles": "20000"}),
        _h(1, "OPTIMIZE", {"predicate": "[\"(date >= '2026-01-01')\"]"}, {"numRemovedFiles": "100"}),
    ]
    s = summarize_history(rows)
    assert s["migration_method"] == "shallow_clone"
    assert s["optimize_predicates"] == ["[\"(date >= '2026-01-01')\"]"]


def test_clustered_optimize_full_detection():
    plain = {"predicate": "[]", "clusterBy": "[\"id\"]", "zOrderBy": "[]"}
    full = dict(plain, clusteringIsFull="true")
    assert not optimize_was_full(plain)
    assert optimize_was_full(full)
    # First enabling clustering (old keys empty) doesn't need FULL: plain OPTIMIZE clusters unclustered data.
    first = {"oldClusteringColumns": "", "newClusteringColumns": "customer_id"}
    assert summarize_history([_h(0, "CLONE", {}), _h(1, "CLUSTER BY", first), _h(2, "OPTIMIZE", plain)]
                             )["recluster_needed"] is False
    # Changing keys does, until an OPTIMIZE FULL runs.
    changed = {"oldClusteringColumns": "customer_id", "newClusteringColumns": "event_date"}
    assert summarize_history([_h(0, "CLUSTER BY", changed), _h(1, "OPTIMIZE", plain)])["recluster_needed"] is True
    assert summarize_history([_h(0, "CLUSTER BY", changed), _h(1, "OPTIMIZE", full)])["recluster_needed"] is False


def test_clone_from_uc_is_not_hive():
    s = summarize_history([_h(0, "CLONE", {"source": "main.raw.orders"}, {"numCopiedFiles": "10"})])
    assert s["clone_source_is_hive"] is False


def test_rewrite_ratio_falls_back_to_po_when_clone_aged_out():
    s = summarize_history([_h(40, "OPTIMIZE", {}, {"numRemovedFiles": "10"})])
    assert rewrite_ratio(s) is None  # no baseline
    s["clone_files"] = 1000  # e.g. known from elsewhere
    assert rewrite_ratio(s, po_compacted_files=250) == 0.25


@pytest.mark.parametrize("rows,expected", [
    ([("Owner", "x", ""), ("Predictive Optimization", "ENABLE (inherited from CATALOG main)", "")],
     {"po_enabled": True, "po_setting": "ENABLE (inherited from CATALOG main)", "po_inherited_from": "inherited from CATALOG main"}),
    ([("Predictive Optimization", "DISABLE", "")], {"po_enabled": False, "po_setting": "DISABLE", "po_inherited_from": None}),
    ([("Owner", "x", "")], {"po_enabled": None, "po_setting": None, "po_inherited_from": None}),
])
def test_parse_po_status(rows, expected):
    assert parse_po_status(rows) == expected


# --- cause classification and severity --------------------------------------------------------

def _rec(**kw):
    base = {"full_name": "main.sales.orders", "name_parts": ("main", "sales", "orders"),
            "num_files": 150_000, "size_bytes": 500 * GB, "avg_file_mb": 3.4,
            "partition_columns": [], "clustering_columns": [], "partition_count": None,
            "history_versions_read": 10, "optimize_runs_after_migration": 0, "optimize_predicates": [],
            "recluster_needed": False, "po_enabled": True, "po_compaction_runs": 0, "migrated": True,
            "migration_label": "deep_clone", "properties": {}}
    base.update(kw)
    return base


@pytest.mark.parametrize("overrides,cause", [
    ({"migration_label": "shallow_clone"}, "shallow_clone"),
    ({"clustering_columns": ["id"], "recluster_needed": True}, "clustering_keys_changed"),
    ({"clustering_columns": ["id"], "recluster_needed": False, "po_compaction_runs": 4}, "po_backlog"),
    ({"partition_columns": ["d"], "partition_count": 5000}, "over_partitioned"),
    ({"optimize_predicates": ["d > 1"], "optimize_runs_after_migration": 3}, "scoped_optimize"),
    ({"po_enabled": False}, "po_disabled"),
    ({"po_enabled": True, "po_compaction_runs": 0}, "po_not_running"),
    ({"po_enabled": True, "po_compaction_runs": 12}, "po_backlog"),
    ({"po_enabled": None}, "no_maintenance"),
    ({"po_enabled": None, "history_versions_read": None}, "unknown"),
])
def test_classify_cause(overrides, cause):
    assert mb.classify_cause(_rec(**overrides), DEFAULTS) == cause


def test_severity():
    assert mb.backlog_severity(_rec(num_files=100_000), DEFAULTS) == "HIGH"
    low_ratio = _rec(num_files=20_000, po_compaction_runs=5, rewrite_ratio=0.1, cause="po_backlog")
    assert mb.backlog_severity(low_ratio, DEFAULTS) == "HIGH"
    assert mb.backlog_severity(dict(low_ratio, rewrite_ratio=0.6), DEFAULTS) == "MEDIUM"
    assert mb.backlog_severity(dict(low_ratio, migrated=False), DEFAULTS) == "MEDIUM"


def test_is_small_files():
    assert mb.is_small_files(_rec(), DEFAULTS)
    assert not mb.is_small_files(_rec(avg_file_mb=128), DEFAULTS)
    assert not mb.is_small_files(_rec(num_files=500), DEFAULTS)
    assert not mb.is_small_files(_rec(size_bytes=GB // 2), DEFAULTS)


def test_provenance_order():
    hist = _rec(migration_method="deep_clone", clone_source_is_hive=True, clone_source="`hive_metastore`.`s`.`o`")
    assert mb.provenance(hist, {}) == {"migrated": True, "migration_label": "deep_clone",
                                       "migration_source": "hive_metastore.s.o"}
    ucx = _rec(migration_method=None, properties={"upgraded_from": "hive_metastore.s.o"})
    assert mb.provenance(ucx, {})["migration_label"] == "ucx"
    lin = _rec(migration_method="ctas")
    assert mb.provenance(lin, {"main.sales.orders": "hive_metastore.s.o"})["migration_label"] == "ctas"
    assert mb.provenance(_rec(migration_method=None), {})["migrated"] is False


def test_clean_record_undoes_pandas_types():
    rec = mb.clean_record({"a": float("nan"), "b": np.bool_(False), "c": np.int64(3), "d": ["x"]})
    assert rec == {"a": None, "b": False, "c": 3, "d": ["x"]}
    assert rec["b"] is False


def test_remediation_sql():
    sql = mb.remediation_sql(_rec(cause="clustering_keys_changed", clustering_columns=["id"]))
    assert sql.rstrip().endswith("OPTIMIZE `main`.`sales`.`orders` FULL;")
    assert "150,000 files" in sql
    plain = mb.remediation_sql(_rec(cause="po_backlog"))
    assert plain.rstrip().endswith("OPTIMIZE `main`.`sales`.`orders`;")


# --- migration_backlog.run on prepared stats (no Spark) ---------------------------------------

def test_run_end_to_end_customer_case():
    """Deep clone from Hive + predictive optimization that has only touched a few percent of the files."""
    ctx = ReviewContext.create(spark=None, skill_dir=SKILL_DIR)
    detail = pd.DataFrame([
        _rec(migration_method="deep_clone", clone_source_is_hive=True, clone_source="hive_metastore.sales.orders",
             migration_version=0, clone_files=150_000, optimize_runs_after_migration=6,
             optimize_files_removed_after_migration=4_000),
        _rec(full_name="main.sales.small", name_parts=("main", "sales", "small"), num_files=50,
             size_bytes=GB, avg_file_mb=20.0),
        _rec(full_name="main.raw.events", name_parts=("main", "raw", "events"), num_files=40_000,
             size_bytes=100 * GB, avg_file_mb=2.5, migration_method=None, po_enabled=None),
    ]).drop(columns=["migrated", "migration_label", "po_compaction_runs"])
    detail.loc[2, "history_versions_read"] = np.nan  # pandas NaN, as after a real merge
    ctx.add_stat("uc", "table_detail", detail)
    ctx.add_stat("uc", "po_activity", pd.DataFrame([{
        "catalog_name": "main", "schema_name": "sales", "table_name": "orders", "operation_type": "COMPACTION",
        "runs": 6, "first_run": None, "last_run": datetime(2026, 9, 1), "dbus": 12.5,
        "files_rewritten": 4000}]))
    ctx.add_stat("uc", "hive_tables", pd.DataFrame([
        {"schema": "sales", "table": "orders", "full_name": "hive_metastore.sales.orders"},
        {"schema": "sales", "table": "returns", "full_name": "hive_metastore.sales.returns"},
    ]))

    backlog = mb.run(ctx)

    assert set(backlog["full_name"]) == {"main.sales.orders", "main.raw.events"}
    by_id = {f.check_id: f for f in ctx.findings}
    mig = by_id["MIG-BACKLOG"]
    assert mig.severity == "HIGH"
    assert mig.evidence[0]["cause"] == "po_backlog"
    assert mig.evidence[0]["rewritten_pct"] == 2.7  # 4,000 of 150,000
    assert mig.remediation_sql[0].rstrip().endswith("OPTIMIZE `main`.`sales`.`orders`;")
    assert by_id["UC-SMALL-FILES"].evidence[0]["cause"] == "unknown"
    hms = by_id["UC-HMS-REMAINING"]
    assert hms.severity == "MEDIUM"
    assert ctx.stats["uc"]["hive_summary"] == {"hive_tables": 2, "with_known_uc_copy": 1, "without_known_uc_copy": 1}
    # One evidence row per Hive table (not per schema), tables without a UC copy first.
    assert hms.evidence == [
        {"full_name": "hive_metastore.sales.returns", "schema": "sales", "uc_copy": None},
        {"full_name": "hive_metastore.sales.orders", "schema": "sales", "uc_copy": "main.sales.orders"},
    ]
    assert hms.title == "2 tables left in hive_metastore (1 with no known Unity Catalog copy)"
    assert mig.title == "1 table migrated from hive_metastore with 150,000 small files left"


def test_po_map_counts_compaction_and_clustering_rewrites():
    po = pd.DataFrame([
        {"catalog_name": "Main", "schema_name": "s", "table_name": "t", "operation_type": "COMPACTION", "runs": 3,
         "last_run": None, "files_rewritten": 100},
        {"catalog_name": "Main", "schema_name": "s", "table_name": "t", "operation_type": "CLUSTERING", "runs": 2,
         "last_run": None, "files_rewritten": 50},
        {"catalog_name": "Main", "schema_name": "s", "table_name": "t", "operation_type": "VACUUM", "runs": 9,
         "last_run": None, "files_rewritten": None},
        {"catalog_name": "main", "schema_name": "s", "table_name": "u", "operation_type": "COMPACTION", "runs": 1,
         "last_run": None, "files_rewritten": float("nan")},
    ])
    m = mb._po_map(po)
    assert m["main.s.t"]["po_compaction_runs"] == 3 and m["main.s.t"]["po_clustering_runs"] == 2
    assert m["main.s.t"]["po_compacted_files"] == 150
    assert m["main.s.u"]["po_compacted_files"] is None


def test_plural():
    assert plural(1, "table") == "1 table"
    assert plural(1500, "table") == "1,500 tables"
    assert plural(2, "catalog, schema, or table", "catalogs, schemas, and tables") == "2 catalogs, schemas, and tables"
