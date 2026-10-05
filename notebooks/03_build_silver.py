# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 — Validate, deduplicate and quarantine
# MAGIC Prerequisite: notebook 02 has completed successfully. Use Serverless compute.
# MAGIC Bronze remains unchanged. Silver publishes a validated snapshot of demo_v1.
# MAGIC Exact duplicates retain one deterministic record. Conflicting records sharing
# MAGIC a business key are all quarantined because no source update policy exists yet.
# MAGIC Each rejected record retains its original payload, lineage and reason codes.
# MAGIC All baseline checks run before publication. Delta writes are atomic per table,
# MAGIC not across tables: run this notebook serially and rerun it after a failed write
# MAGIC before executing the gold notebook. This is a small demo snapshot, not CDC.

# COMMAND ----------

import json
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
from pathlib import Path
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "customer_insights_dev"
DATASET_VERSION = "demo_v1"
RUN_ID = str(uuid4())
EVALUATED_AT = datetime.now(timezone.utc)
manifest = json.loads(Path(f"/Volumes/{CATALOG}/bronze/landing/{DATASET_VERSION}/manifest.json").read_text())
AS_OF = F.lit(manifest["as_of_date"]).cast("date")
KEYS = {"customers": "customer_id", "invoices": "invoice_id", "payments": "payment_id"}

# COMMAND ----------

def blank(name):
    return F.col(name).isNull() | (F.trim(F.col(name)) == "")


def load_raw(entity):
    table = f"{CATALOG}.bronze.{entity}_raw"
    raw = spark.table(table).filter(F.col("_dataset_version") == DATASET_VERSION)
    assert raw.count() == manifest["source_row_counts"][entity], f"Unexpected baseline source count: {table}"
    payload_columns = [name for name in raw.columns if not name.startswith("_")]
    return raw.withColumn("_raw_json", F.to_json(F.struct(*[F.col(name) for name in payload_columns])))


def validate(entity, frame, rules):
    key = KEYS[entity]
    versions = frame.groupBy(key).agg(F.countDistinct("_record_hash").alias("_key_versions"))
    frame = frame.join(versions, key, "left")
    rules = [
        (blank(key), f"missing_{key}"),
        (blank("source_record_id"), "missing_source_record_id"),
        (F.col("_rescued_data").isNotNull(), "unexpected_source_schema"),
        (F.col("_key_versions") > 1, f"conflicting_{key}"),
    ] + rules
    reason_expressions = [F.when(F.coalesce(condition, F.lit(False)), F.lit(code)) for condition, code in rules]
    frame = frame.withColumn("_reasons", F.filter(F.array(*reason_expressions), lambda value: value.isNotNull()))
    order = Window.partitionBy(key).orderBy(F.size("_reasons"), "source_record_id", "_raw_record_id")
    frame = frame.withColumn("_duplicate_rank", F.row_number().over(order))
    duplicate = (F.size("_reasons") == 0) & (F.col("_duplicate_rank") > 1)
    frame = frame.withColumn("_reasons", F.concat(
        F.col("_reasons"),
        F.when(duplicate, F.array(F.lit(f"duplicate_{key}"))).otherwise(F.array().cast("array<string>")),
    ))
    clean = (
        frame.filter(F.size("_reasons") == 0)
        .drop("_raw_json", "_amount_text", "_key_versions", "_duplicate_rank", "_reasons", "_customer_exists", "_invoice_issue_date")
        .withColumn("_silver_run_id", F.lit(RUN_ID))
        .withColumn("_validated_at", F.lit(EVALUATED_AT))
    )
    quarantine = frame.filter(F.size("_reasons") > 0).select(
        F.lit(entity).alias("entity"),
        F.col(key).alias("business_key"),
        "source_record_id", "_raw_record_id", "_source_file", "_ingested_at", "_dataset_version",
        F.col("_raw_json").alias("raw_record_json"),
        F.col("_reasons").alias("reason_codes"),
        F.lit(RUN_ID).alias("quality_run_id"),
        F.lit(EVALUATED_AT).alias("evaluated_at"),
    )
    return clean, quarantine


def parse_money(frame):
    return frame.withColumn("_amount_text", F.col("amount_gbp")).withColumn(
        "amount_gbp", F.expr("try_cast(amount_gbp AS DECIMAL(12,2))")
    )


def money_rules(entity):
    return [
        (F.col("amount_gbp").isNull() | (F.col("amount_gbp") <= 0), f"{entity}_amount_not_positive"),
        # Reject precision loss instead of silently rounding a source value.
        (~F.col("_amount_text").rlike(r"^-?\d+(\.\d{1,2})?$"), f"{entity}_money_format_invalid"),
    ]

# COMMAND ----------

customers_raw = load_raw("customers").withColumn("joined_date", F.expr("try_cast(joined_date AS DATE)"))
customers, rejected_customers = validate("customers", customers_raw, [
    (blank("customer_name"), "customer_name_missing"),
    (blank("email") | ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), "customer_email_invalid"),
    (blank("region"), "customer_region_missing"),
    (F.col("joined_date").isNull() | (F.col("joined_date") > AS_OF), "customer_joined_date_invalid"),
])

invoices_raw = (
    parse_money(load_raw("invoices"))
    .withColumn("issue_date", F.expr("try_cast(issue_date AS DATE)"))
    .withColumn("due_date", F.expr("try_cast(due_date AS DATE)"))
    .join(customers.select("customer_id").withColumn("_customer_exists", F.lit(True)), "customer_id", "left")
)
invoices, rejected_invoices = validate("invoices", invoices_raw, money_rules("invoice") + [
    (F.col("_customer_exists").isNull(), "invoice_customer_not_found"),
    (F.col("issue_date").isNull() | (F.col("issue_date") > AS_OF), "invoice_issue_date_invalid"),
    (F.col("due_date").isNull() | (F.col("due_date") < F.col("issue_date")), "invoice_due_date_invalid"),
])

payments_raw = (
    parse_money(load_raw("payments"))
    .withColumn("payment_date", F.expr("try_cast(payment_date AS DATE)"))
    .join(invoices.select("invoice_id", F.col("issue_date").alias("_invoice_issue_date")), "invoice_id", "left")
)
payments, rejected_payments = validate("payments", payments_raw, money_rules("payment") + [
    (F.col("_invoice_issue_date").isNull(), "payment_invoice_not_found"),
    (F.col("payment_date").isNull() | (F.col("payment_date") > AS_OF), "payment_date_invalid"),
    (F.col("payment_date") < F.col("_invoice_issue_date"), "payment_before_invoice"),
])

clean_frames = {"customers": customers, "invoices": invoices, "payments": payments}
rejected_frames = {"customers": rejected_customers, "invoices": rejected_invoices, "payments": rejected_payments}
quarantine = reduce(lambda left, right: left.unionByName(right), rejected_frames.values())
rule_counts = quarantine.select(F.explode("reason_codes").alias("rule_code")).groupBy("rule_code").count().withColumnRenamed("count", "excluded_records")

# COMMAND ----------

# Gate publication on counts, uniqueness, source conservation and monetary totals.
summary_rows = []
for entity, frame in clean_frames.items():
    key = KEYS[entity]
    clean_stats = frame.agg(F.count("*").alias("rows"), F.countDistinct(key).alias("keys")).first()
    clean_count = clean_stats["rows"]
    rejected_count = rejected_frames[entity].count()
    input_count = manifest["source_row_counts"][entity]
    assert clean_count == manifest["expected_clean_row_counts"][entity], f"{entity}: unexpected clean count"
    assert clean_stats["keys"] == clean_count, f"{entity}: duplicate or null clean keys"
    assert clean_count + rejected_count == input_count, f"{entity}: records were lost or multiplied"
    accepted_ids = frame.select("_raw_record_id")
    rejected_ids = rejected_frames[entity].select("_raw_record_id")
    assert accepted_ids.join(rejected_ids, "_raw_record_id", "inner").limit(1).count() == 0, f"{entity}: accepted/rejected overlap"
    assert accepted_ids.unionByName(rejected_ids).distinct().count() == input_count, f"{entity}: source identity mismatch"
    summary_rows.append((entity, input_count, clean_count, rejected_count, RUN_ID, manifest["as_of_date"]))

observed_rules = {row["rule_code"]: row["excluded_records"] for row in rule_counts.collect()}
assert observed_rules == manifest["expected_excluded_records"], f"Unexpected rule results: {observed_rules}"
assert quarantine.count() == 105, "Unexpected quarantine count for demo_v1"

invoiced = invoices.agg(F.sum("amount_gbp").alias("total")).first()["total"]
received = payments.agg(F.sum("amount_gbp").alias("total")).first()["total"]
expected_metrics = manifest["expected_clean_metrics"]
assert invoiced == Decimal(expected_metrics["invoiced_gbp"]), "Invoice total does not reconcile"
assert received == Decimal(expected_metrics["received_gbp"]), "Payment total does not reconcile"
assert invoiced - received == Decimal(expected_metrics["outstanding_gbp"]), "Outstanding balance does not reconcile"
print(f"All pre-publication checks passed. Invoiced: GBP {invoiced:,.2f}; received: GBP {received:,.2f}.")

# COMMAND ----------

summary = spark.createDataFrame(summary_rows,
    "entity string, source_rows long, accepted_rows long, excluded_rows long, quality_run_id string, as_of_date string"
).withColumn("as_of_date", F.col("as_of_date").cast("date")).withColumn("evaluated_at", F.lit(EVALUATED_AT))
quality_rules = rule_counts.withColumn("quality_run_id", F.lit(RUN_ID)).withColumn("evaluated_at", F.lit(EVALUATED_AT))

outputs = {
    **clean_frames,
    "quarantine": quarantine,
    "quality_summary": summary,
    "quality_rules": quality_rules,
}
for name, frame in outputs.items():
    target = f"{CATALOG}.silver.{name}"
    frame.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"PUBLISHED {target}")

# Verify persisted counts after the table writes.
for entity in KEYS:
    assert spark.table(f"{CATALOG}.silver.{entity}").count() == manifest["expected_clean_row_counts"][entity]
assert spark.table(f"{CATALOG}.silver.quarantine").count() == 105
print("Silver publication verified. Reruns replace the snapshot; they do not append duplicate outputs.")

# COMMAND ----------

display(spark.table(f"{CATALOG}.silver.quality_summary").orderBy("entity"))
display(spark.table(f"{CATALOG}.silver.quality_rules").orderBy("rule_code"))
display(spark.table(f"{CATALOG}.silver.quarantine").select(
    "entity", "business_key", "reason_codes", "source_record_id", "raw_record_json"
).orderBy("entity", "business_key").limit(15))