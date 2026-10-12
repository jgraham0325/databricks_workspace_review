# Databricks notebook source
# Drops the objects created by seed_migration_scenario, and nothing else.
import json
MARK = "workspace-review seed"
dbutils.widgets.text("uc_catalog", "")
UC_CATALOG = dbutils.widgets.get("uc_catalog").strip()
if not UC_CATALOG:
    raise ValueError("Set the uc_catalog job parameter used by seed_migration_scenario.")
out = {}
for catalog in [UC_CATALOG, "hive_metastore"]:
    schema = f"{catalog}.wr_seed"
    if not spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE 'wr_seed'").count():
        out[schema] = "not found"
        continue
    rows = spark.sql(f"DESCRIBE SCHEMA EXTENDED {schema}").collect()
    comment = next((r[1] for r in rows if r[0].lower() in ("comment", "description")), None)
    if comment != MARK:
        out[schema] = f"SKIPPED: comment is {comment!r}, not the seed marker"
        continue
    tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {schema}").collect() if not r["isTemporary"]]
    try:
        for t in tables:
            spark.sql(f"DROP TABLE {schema}.`{t}`")
        spark.sql(f"DROP SCHEMA {schema}")
        out[schema] = {"dropped_tables": tables, "dropped_schema": True}
    except Exception as e:
        out[schema] = {"tables": tables, "error": str(e)[:500]}
dbutils.notebook.exit(json.dumps(out))
