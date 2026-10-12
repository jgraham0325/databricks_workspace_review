# Databricks notebook source
# MAGIC %md
# MAGIC # Seed: Hive → UC migration with a small-file backlog
# MAGIC Creates test objects for the workspace-review skill, **only** inside `hive_metastore.wr_seed` and
# MAGIC the `wr_seed` schema of the `uc_catalog` parameter. Re-runnable: drops and recreates its own tables.
# MAGIC
# MAGIC | UC table | Setup | Expected cause |
# MAGIC |---|---|---|
# MAGIC | `orders_scoped` | deep clone of a partitioned Hive table, then `OPTIMIZE ... WHERE` on the last 3 days | `scoped_optimize` |
# MAGIC | `orders_clustered` | deep clone, then `ALTER TABLE CLUSTER BY`, then plain `OPTIMIZE` | `clustered_without_full` (if plain OPTIMIZE really skips old data) |
# MAGIC | `orders_untouched` | deep clone, nothing else (PO enabled, inherited) | `po_not_running` |

# COMMAND ----------

import json
MARK = "workspace-review seed"
dbutils.widgets.text("uc_catalog", "")
UC_CATALOG = dbutils.widgets.get("uc_catalog").strip()
if not UC_CATALOG:
    raise ValueError("Set the uc_catalog job parameter to a writable Unity Catalog catalog.")
HIVE, UC = "hive_metastore.wr_seed", f"{UC_CATALOG}.wr_seed"
FILES = 3000

# Keep writes fragmented, the way the customer's Hive tables were.
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")


def comment_of(schema):
    rows = spark.sql(f"DESCRIBE SCHEMA EXTENDED {schema}").collect()
    return next((r[1] for r in rows if r[0].lower() in ("comment", "description")), None)


for schema, show in [(HIVE, "SHOW SCHEMAS IN hive_metastore LIKE 'wr_seed'"),
                     (UC, f"SHOW SCHEMAS IN `{UC_CATALOG}` LIKE 'wr_seed'")]:
    if spark.sql(show).count() and comment_of(schema) != MARK:
        raise Exception(f"{schema} exists and wasn't created by this seed; refusing to touch it")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{MARK}'")

# COMMAND ----------

from pyspark.sql import functions as F

no_compaction = ("TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', "
                 "'delta.autoOptimize.autoCompact' = 'false')")
base = (spark.range(0, 600_000)
        .withColumn("customer_id", (F.col("id") % 5000).cast("int"))
        .withColumn("amount", (F.rand(7) * 1000).cast("decimal(10,2)"))
        .withColumn("event_date", F.expr("date_add(date'2026-09-01', cast(id % 30 as int))")))

for t in ["orders_hive_partitioned", "orders_hive_flat"]:
    spark.sql(f"DROP TABLE IF EXISTS {HIVE}.{t}")
for t in ["orders_scoped", "orders_clustered", "orders_untouched"]:
    spark.sql(f"DROP TABLE IF EXISTS {UC}.{t}")

# Partitioned Hive table: 30 partitions x 100 files. Flat Hive table: 3,000 files.
spark.sql(f"CREATE TABLE {HIVE}.orders_hive_partitioned (id BIGINT, customer_id INT, amount DECIMAL(10,2), "
          f"event_date DATE) USING DELTA PARTITIONED BY (event_date) {no_compaction}")
(base.repartition(FILES, "event_date", (F.col("id") % 100))
     .write.mode("append").insertInto(f"{HIVE}.orders_hive_partitioned"))

spark.sql(f"CREATE TABLE {HIVE}.orders_hive_flat (id BIGINT, customer_id INT, amount DECIMAL(10,2), "
          f"event_date DATE) USING DELTA {no_compaction}")
base.repartition(FILES).write.mode("append").insertInto(f"{HIVE}.orders_hive_flat")

# COMMAND ----------

# Migrate to UC managed tables the way the customer did: DEEP CLONE.
spark.sql(f"CREATE TABLE {UC}.orders_scoped DEEP CLONE {HIVE}.orders_hive_partitioned")
spark.sql(f"CREATE TABLE {UC}.orders_clustered DEEP CLONE {HIVE}.orders_hive_flat")
spark.sql(f"CREATE TABLE {UC}.orders_untouched DEEP CLONE {HIVE}.orders_hive_flat")

# Partial maintenance.
spark.sql(f"OPTIMIZE {UC}.orders_scoped WHERE event_date >= date'2026-09-28'")
spark.sql(f"ALTER TABLE {UC}.orders_clustered CLUSTER BY (customer_id)")
spark.sql(f"OPTIMIZE {UC}.orders_clustered")

# COMMAND ----------

# Record what was actually created, so the review's numbers can be checked against ground truth.
truth = {}
for t in [f"{HIVE}.orders_hive_partitioned", f"{HIVE}.orders_hive_flat",
          f"{UC}.orders_scoped", f"{UC}.orders_clustered", f"{UC}.orders_untouched"]:
    d = spark.sql(f"DESCRIBE DETAIL {t}").first()
    hist = spark.sql(f"DESCRIBE HISTORY {t}").collect()
    truth[t] = {
        "numFiles": d["numFiles"], "sizeInBytes": d["sizeInBytes"],
        "clusteringColumns": list(d["clusteringColumns"] or []), "partitionColumns": list(d["partitionColumns"] or []),
        "history": [{"v": h["version"], "op": h["operation"], "params": dict(h["operationParameters"] or {}),
                     "metrics": {k: v for k, v in (h["operationMetrics"] or {}).items()
                                 if k in ("numRemovedFiles", "numAddedFiles", "sourceNumOfFiles", "numCopiedFiles")}}
                    for h in hist],
    }
    if t.startswith(f"{UC_CATALOG}."):
        ext = spark.sql(f"DESCRIBE TABLE EXTENDED {t}").collect()
        truth[t]["extended_po_rows"] = [[r[0], r[1]] for r in ext if "predictive" in (r[0] or "").lower()]
dbutils.notebook.exit(json.dumps(truth, default=str))
