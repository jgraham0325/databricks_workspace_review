"""Step 1: Unity Catalog inventory, hive_metastore inventory, and inventory-level checks."""

from __future__ import annotations

import pandas as pd

from .checks import CheckSpec
from .context import Finding, ReviewContext, evidence_records, plural, principal_kind, quote_ident

AREA = "uc"

# information_schema table types / formats that hold data files we can inspect.
DATA_TABLE_TYPES = {"MANAGED", "EXTERNAL"}
NON_DELTA_FILE_FORMATS = {"PARQUET", "CSV", "JSON", "AVRO", "ORC", "TEXT", "BINARYFILE"}

# Column (or expression) holding the catalog name in each query of uc_assets.sql.
CATALOG_COLUMNS = {
    "catalogs": "catalog_name",
    "schemas": "catalog_name",
    "tables": "table_catalog",
    "volumes": "volume_catalog",
    "hive_lineage_targets": "substring_index(target_table_full_name, '.', 1)",
    "po_activity": "catalog_name",
}


def run(ctx: ReviewContext) -> None:
    collect(ctx)
    evaluate(ctx)


def collect(ctx: ReviewContext) -> None:
    if not ctx.sources.get("information_schema"):
        ctx.note("Unity Catalog inventory skipped: system.information_schema is not readable.")
        return

    q = {name: ctx.render(sql, catalog_col=CATALOG_COLUMNS[name])
         for name, sql in ctx.queries("uc_assets.sql").items()}
    catalogs = ctx.query_pdf(q["catalogs"])
    schemas = ctx.query_pdf(q["schemas"])
    tables = ctx.query_pdf(q["tables"])
    volumes = ctx.query_pdf(q["volumes"])

    missing = sorted(set(ctx.included_catalogs()) - set(catalogs["catalog_name"].str.lower()) - {"hive_metastore"})
    if missing:
        ctx.note(f"Requested catalogs not found or not visible to {ctx.current_user}: {', '.join(missing)}")
    tables["full_name"] = tables["table_catalog"] + "." + tables["table_schema"] + "." + tables["table_name"]

    for name, df in [("catalogs", catalogs), ("schemas", schemas), ("tables", tables), ("volumes", volumes)]:
        ctx.add_stat(AREA, name, df)

    ctx.add_stat(AREA, "counts", {
        "catalogs": len(catalogs),
        "schemas": len(schemas),
        "tables_and_views": len(tables),
        "volumes": len(volumes),
    })
    ctx.add_stat(AREA, "tables_by_type", _value_counts(tables, "table_type"))
    ctx.add_stat(AREA, "tables_by_format", _value_counts(tables, "data_source_format"))
    ctx.add_stat(AREA, "tables_per_catalog", _value_counts(tables, "table_catalog"))

    if ctx.sources.get("table_lineage"):
        try:
            ctx.add_stat(AREA, "hive_lineage_targets", ctx.query_pdf(q["hive_lineage_targets"]))
        except Exception as exc:
            ctx.record_error("system.access.table_lineage", "uc_assets", exc)
    if ctx.sources.get("predictive_optimization"):
        try:
            ctx.add_stat(AREA, "po_activity", ctx.query_pdf(q["po_activity"]))
        except Exception as exc:
            ctx.record_error("system.storage.predictive_optimization_operations_history", "uc_assets", exc)

    if ctx.sources.get("hive_metastore") and ctx.catalog_in_scope("hive_metastore"):
        ctx.add_stat(AREA, "hive_tables", _hive_inventory(ctx))


def evaluate(ctx: ReviewContext) -> None:
    for check in CHECKS:
        check.evaluate(ctx)


def _inventory(ctx):
    st = ctx.stats.get(AREA, {})
    return st.get("catalogs"), st.get("schemas"), st.get("tables"), st.get("volumes")


def _run_non_delta(ctx):
    _, _, tables, _ = _inventory(ctx)
    if tables is not None:
        _check_non_delta(ctx, tables)


def _run_owners(ctx):
    catalogs, schemas, tables, _ = _inventory(ctx)
    if all(df is not None for df in (catalogs, schemas, tables)):
        _check_user_owners(ctx, catalogs, schemas, tables)


def _run_comments(ctx):
    _, _, tables, _ = _inventory(ctx)
    if tables is not None:
        _check_missing_comments(ctx, tables)


def _run_empty_schemas(ctx):
    _, schemas, tables, volumes = _inventory(ctx)
    if all(df is not None for df in (schemas, tables, volumes)):
        _check_empty_schemas(ctx, schemas, tables, volumes)


def _value_counts(df: pd.DataFrame, col: str) -> dict:
    if df.empty:
        return {}
    return df[col].fillna("(none)").value_counts().to_dict()


def _hive_inventory(ctx: ReviewContext) -> pd.DataFrame:
    rows = []
    schemas = ctx.run_sql("SHOW SCHEMAS IN hive_metastore").collect()
    for s in schemas:
        db = s[0]  # column is databaseName or namespace depending on runtime
        try:
            for t in ctx.run_sql(f"SHOW TABLES IN {quote_ident('hive_metastore', db)}").collect():
                if not t["isTemporary"]:
                    rows.append({"schema": db, "table": t["tableName"],
                                 "full_name": f"hive_metastore.{db}.{t['tableName']}"})
        except Exception as exc:
            ctx.record_error(f"hive_metastore.{db}", "hive inventory", exc)
    return pd.DataFrame(rows, columns=["schema", "table", "full_name"])


def _check_non_delta(ctx: ReviewContext, tables: pd.DataFrame) -> None:
    fmt = tables["data_source_format"].fillna("").str.upper()
    non_delta = tables[tables["table_type"].isin(DATA_TABLE_TYPES) & fmt.isin(NON_DELTA_FILE_FORMATS)]
    if non_delta.empty:
        return
    by_fmt = non_delta["data_source_format"].value_counts().to_dict()
    ctx.add_finding(Finding(
        check_id="UC-NON-DELTA",
        area=AREA,
        severity="MEDIUM" if len(non_delta) >= 20 else "LOW",
        title=f"{plural(len(non_delta), 'table')} not stored as Delta",
        summary="Formats: " + ", ".join(f"{k} ({v})" for k, v in by_fmt.items()),
        impact="Non-Delta tables miss ACID transactions, time travel, data skipping, and predictive optimization.",
        recommendation="Convert Parquet tables in place with CONVERT TO DELTA; rewrite other formats with CTAS "
                       "into a managed Delta table.",
        evidence=evidence_records(non_delta[["full_name", "table_type", "data_source_format", "table_owner"]]),
        doc_url="https://docs.databricks.com/sql/language-manual/delta-convert-to-delta.html",
    ))


CHECKS = (
    CheckSpec("UC-NON-DELTA", AREA, "uc_assets", _run_non_delta),
    CheckSpec("UC-OWNER-USER", AREA, "uc_assets", _run_owners),
    CheckSpec("UC-NO-COMMENT", AREA, "uc_assets", _run_comments, ("missing_comment_low_pct",)),
    CheckSpec("UC-EMPTY-SCHEMA", AREA, "uc_assets", _run_empty_schemas),
)


def _check_user_owners(ctx, catalogs, schemas, tables) -> None:
    rows = []
    for kind, df, name_cols, owner_col in [
        ("catalog", catalogs, ["catalog_name"], "catalog_owner"),
        ("schema", schemas, ["catalog_name", "schema_name"], "schema_owner"),
        ("table", tables, ["full_name"], "table_owner"),
    ]:
        for rec in df.to_dict("records"):
            owner = rec.get(owner_col)
            if principal_kind(owner) == "user":
                rows.append({"object_type": kind, "object": ".".join(rec[c] for c in name_cols), "owner": owner})
    if not rows:
        return
    ev = pd.DataFrame(rows)
    counts = ev["object_type"].value_counts().to_dict()
    ctx.add_finding(Finding(
        check_id="UC-OWNER-USER",
        area=AREA,
        severity="MEDIUM" if counts.get("catalog", 0) or counts.get("schema", 0) >= 10 else "LOW",
        title=f"{plural(len(ev), 'catalog, schema, or table', 'catalogs, schemas, and tables')} owned by individual users",
        summary=", ".join(f"{v} {k}s" for k, v in counts.items()) + f"; {ev['owner'].nunique()} distinct users.",
        impact="When the owner leaves or changes role, nobody else can manage grants or drop the object.",
        recommendation="Transfer ownership to a group or service principal "
                       "(ALTER ... OWNER TO `group`). Prioritise catalogs and schemas.",
        evidence=ev.sort_values("object_type").to_dict("records"),
    ))


def _check_missing_comments(ctx, tables) -> None:
    data = tables[tables["table_type"] != "VIEW"]
    if data.empty:
        return
    missing = data[data["comment"].isna() | (data["comment"].fillna("").str.strip() == "")]
    pct = 100.0 * len(missing) / len(data)
    ctx.add_stat(AREA, "pct_tables_without_comment", round(pct, 1))
    if pct < ctx.config["missing_comment_low_pct"]:
        return
    ctx.add_finding(Finding(
        check_id="UC-NO-COMMENT",
        area=AREA,
        severity="LOW",
        title=f"{pct:.0f}% of tables have no description",
        summary=f"{len(missing)} of {len(data)} tables have no comment.",
        impact="Undocumented tables are harder to discover and are described poorly by Genie and the Assistant.",
        recommendation="Add table comments, starting with the most queried tables. AI-generated comments in "
                       "Catalog Explorer can draft them for review.",
        evidence=missing[["full_name", "table_owner"]].to_dict("records"),
    ))


def _check_empty_schemas(ctx, schemas, tables, volumes) -> None:
    used = set(zip(tables["table_catalog"], tables["table_schema"])) | \
        set(zip(volumes["volume_catalog"], volumes["volume_schema"]))
    empty = schemas[[(c, s) not in used for c, s in zip(schemas["catalog_name"], schemas["schema_name"])]]
    empty = empty[empty["schema_name"] != "default"]
    if empty.empty:
        return
    ctx.add_finding(Finding(
        check_id="UC-EMPTY-SCHEMA",
        area=AREA,
        severity="LOW",
        title=f"{plural(len(empty), 'empty schema')} (no tables or volumes)",
        summary="Empty schemas (excluding `default`) visible to the reviewer.",
        impact="Clutter makes the catalog harder to navigate; may indicate abandoned projects.",
        recommendation="Confirm with owners and drop schemas that are no longer needed.",
        evidence=empty[["catalog_name", "schema_name", "schema_owner", "created"]]
            .astype(str).to_dict("records"),
    ))
