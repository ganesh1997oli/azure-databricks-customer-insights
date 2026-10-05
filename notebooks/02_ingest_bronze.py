# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Incremental ingestion into bronze Delta tables
# MAGIC Run `01_generate_demo_data` first. Attach this notebook to Serverless compute.
# MAGIC Source columns stay as strings; validation and typed conversion happen in silver.
# MAGIC Each dataset has its own persistent schema and streaming checkpoint directories.
# MAGIC `AvailableNow` ingests the available files and then stops the stream.
# MAGIC Run this notebook twice with unchanged source files to verify stable table counts.
# MAGIC Keep checkpoint directories and their target tables together. Do not reset either
# MAGIC independently: removing checkpoints can cause re-ingestion, and dropping tables
# MAGIC can leave checkpoints pointing to data that no longer exists.

# COMMAND ----------

import csv
import hashlib
import json
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = "customer_insights_dev"
DATASET_VERSION = "demo_v1"
LANDING_ROOT = Path(f"/Volumes/{CATALOG}/bronze/landing/{DATASET_VERSION}")
STATE_ROOT = Path(f"/Volumes/{CATALOG}/bronze/ingestion_state/{DATASET_VERSION}")

SOURCE_COLUMNS = {
    "customers": ["customer_id", "customer_name", "email", "region", "joined_date", "source_record_id"],
    "invoices": ["invoice_id", "customer_id", "issue_date", "due_date", "amount_gbp", "source_record_id"],
    "payments": ["payment_id", "invoice_id", "payment_date", "amount_gbp", "source_record_id"],
}

# COMMAND ----------

# Fail before ingesting if the fixtures differ from the generator manifest.
manifest_path = LANDING_ROOT / "manifest.json"
if not manifest_path.exists():
    raise FileNotFoundError(f"Run notebook 01 first. Missing {manifest_path}")
manifest = json.loads(manifest_path.read_text())
assert manifest["dataset_version"] == DATASET_VERSION
assert manifest["synthetic"] is True

for entity, columns in SOURCE_COLUMNS.items():
    relative_path = f"{entity}/batch_001.csv"
    path = LANDING_ROOT / relative_path
    assert path.exists(), f"Missing source file: {path}"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == manifest["source_sha256"][relative_path], \
        f"Source checksum mismatch: {path}"
    with path.open(newline="", encoding="utf-8") as handle:
        assert next(csv.reader(handle)) == columns, f"Unexpected CSV header: {path}"

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.ingestion_state
COMMENT 'Persistent Auto Loader schemas and checkpoints; separate from source files'
""")
print("Source checksums and CSV headers verified.")

# COMMAND ----------

def ingest_entity(entity):
    source_columns = SOURCE_COLUMNS[entity]
    schema = StructType([StructField(name, StringType(), True) for name in source_columns])
    source_path = str(LANDING_ROOT / entity)
    checkpoint_path = str(STATE_ROOT / entity / "checkpoint")
    schema_path = str(STATE_ROOT / entity / "schema")
    target_table = f"{CATALOG}.bronze.{entity}_raw"
    before_count = spark.table(target_table).count() if spark.catalog.tableExists(target_table) else 0

    source = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.allowOverwrites", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("pathGlobFilter", "*.csv")
        .schema(schema)
        .load(source_path)
    )

    business_columns = [name for name in source_columns if name != "source_record_id"]
    bronze = (
        source.select(
            *[F.col(name) for name in source_columns],
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_modified_at"),
        )
        .withColumn("_dataset_version", F.lit(DATASET_VERSION))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct(*[F.col(name) for name in business_columns])), 256))
        .withColumn("_raw_record_id", F.sha2(F.concat_ws("|", F.col("_source_file"), F.col("source_record_id")), 256))
    )

    query = (
        bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .queryName(f"ingest_{DATASET_VERSION}_{entity}")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()
    after_count = spark.table(target_table).count()
    print(f"{target_table}: {before_count:,} -> {after_count:,} rows; added {after_count-before_count:,}")
    return (entity, target_table, before_count, after_count, after_count - before_count)


run_summary = [ingest_entity(entity) for entity in SOURCE_COLUMNS]
display(spark.createDataFrame(
    run_summary,
    "entity string, target_table string, rows_before long, rows_after long, rows_added long",
))

# COMMAND ----------

# Baseline validation deliberately expects only the original demo_v1 batches.
# Extend the manifest/checks when introducing further incremental source batches.
for entity in SOURCE_COLUMNS:
    table = f"{CATALOG}.bronze.{entity}_raw"
    stats = spark.table(table).agg(
        F.count("*").alias("row_count"),
        F.countDistinct("_raw_record_id").alias("unique_raw_ids"),
        F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_rows"),
        F.sum(F.when(F.col("source_record_id").isNull() | (F.trim(F.col("source_record_id")) == ""), 1).otherwise(0)).alias("missing_source_ids"),
        F.sum(F.when(F.col("_source_file").isNull(), 1).otherwise(0)).alias("missing_source_files"),
    ).first()
    expected = manifest["source_row_counts"][entity]
    assert stats["row_count"] == expected, f"{table}: expected {expected}, got {stats['row_count']}"
    assert stats["unique_raw_ids"] == expected, f"{table}: source records were ingested more than once"
    assert stats["rescued_rows"] == 0, f"{table}: inspect unexpected schema fields in _rescued_data"
    assert stats["missing_source_ids"] == 0, f"{table}: missing source record identifiers"
    assert stats["missing_source_files"] == 0, f"{table}: missing lineage metadata"
    print(f"VERIFIED {table}: {expected:,} traceable raw records")

print("Bronze validation passed. Rerun this notebook: rows_added should be 0 for all three datasets.")

# COMMAND ----------

# This duplicate is intentional and must remain in bronze for the silver rule.
display(spark.sql(f"""
SELECT customer_id, COUNT(*) AS source_record_count,
       COUNT(DISTINCT _record_hash) AS distinct_business_records
FROM {CATALOG}.bronze.customers_raw
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_id
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customer_insights_dev.bronze.customers_raw
# MAGIC LIMIT 20;