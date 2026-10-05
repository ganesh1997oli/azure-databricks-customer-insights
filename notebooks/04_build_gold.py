# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 — Publish business and data-quality datasets
# MAGIC Run notebook 03 successfully first. Use Serverless and Run all.
# MAGIC This notebook publishes demo_v1 snapshots for the application screens.
# MAGIC Amounts use decimal GBP. Payments are aggregated before joining invoices,
# MAGIC so multiple payments cannot multiply the invoiced amount.
# MAGIC Outstanding is money owed; it is not proof of revenue leakage.
# MAGIC Overdue uses the fixed manifest date, not today's date. Monthly figures
# MAGIC group by invoice issue month: they are invoice cohorts, not cash-flow months.
# MAGIC Run notebooks serially. Delta publication is atomic per table, not across
# MAGIC tables. After a failed write, rerun this notebook before using the outputs.

# COMMAND ----------

import json
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
from pathlib import Path
from uuid import uuid4
from pyspark.sql import functions as F

CATALOG = "customer_insights_dev"
DATASET_VERSION = "demo_v1"
GOLD_RUN_ID = str(uuid4())
BUILT_AT = datetime.now(timezone.utc)
manifest = json.loads(Path(f"/Volumes/{CATALOG}/bronze/landing/{DATASET_VERSION}/manifest.json").read_text())
AS_OF = F.lit(manifest["as_of_date"]).cast("date")
ZERO = F.lit("0.00").cast("decimal(18,2)")
MONEY_COLUMNS = ["invoiced_gbp", "received_gbp", "outstanding_gbp", "credit_gbp", "overdue_gbp"]

silver = {name: spark.table(f"{CATALOG}.silver.{name}") for name in (
    "customers", "invoices", "payments", "quarantine", "quality_summary", "quality_rules"
)}
summary_rows = silver["quality_summary"].collect()
assert len(summary_rows) == 3, "Rerun notebook 03: incomplete quality summary"
run_ids = {row["quality_run_id"] for row in summary_rows}
assert len(run_ids) == 1 and None not in run_ids, "Rerun notebook 03: mixed silver runs"
SILVER_RUN_ID = next(iter(run_ids))
for name, frame in silver.items():
    run_column = "_silver_run_id" if name in ("customers", "invoices", "payments") else "quality_run_id"
    observed_ids = {row[run_column] for row in frame.select(run_column).distinct().collect()}
    assert observed_ids == {SILVER_RUN_ID}, f"Rerun notebook 03: {name} is from a different silver run"

observed_summary = {row["entity"]: row for row in summary_rows}
assert set(observed_summary) == {"customers", "invoices", "payments"}
for entity, key in {"customers": "customer_id", "invoices": "invoice_id", "payments": "payment_id"}.items():
    frame = silver[entity]
    expected = manifest["expected_clean_row_counts"][entity]
    stats = frame.agg(F.count("*").alias("rows"), F.countDistinct(key).alias("keys")).first()
    assert stats["rows"] == stats["keys"] == expected, f"Invalid silver keys/count: {entity}"
    assert frame.filter(F.col("_dataset_version") != DATASET_VERSION).limit(1).count() == 0
    row = observed_summary[entity]
    assert row["accepted_rows"] == expected and row["source_rows"] == manifest["source_row_counts"][entity]
    assert row["excluded_rows"] == row["source_rows"] - row["accepted_rows"]
    assert row["as_of_date"].isoformat() == manifest["as_of_date"]

expected_rules = manifest["expected_excluded_records"]
assert {row["rule_code"]: row["excluded_records"] for row in silver["quality_rules"].collect()} == expected_rules
assert silver["quarantine"].count() == sum(row["excluded_rows"] for row in summary_rows) == 105
customers, invoices, payments = (silver[name] for name in ("customers", "invoices", "payments"))
assert invoices.join(customers.select("customer_id"), "customer_id", "left_anti").limit(1).count() == 0
assert payments.join(invoices.select("invoice_id"), "invoice_id", "left_anti").limit(1).count() == 0
print(f"Silver prerequisites verified: run {SILVER_RUN_ID}.")

# COMMAND ----------

def stamp(frame):
    return (frame.withColumn("as_of_date", AS_OF)
        .withColumn("_dataset_version", F.lit(DATASET_VERSION))
        .withColumn("_silver_run_id", F.lit(SILVER_RUN_ID))
        .withColumn("_gold_run_id", F.lit(GOLD_RUN_ID))
        .withColumn("_built_at", F.lit(BUILT_AT)))


def money_sums():
    return [F.sum(name).cast("decimal(18,2)").alias(name) for name in MONEY_COLUMNS]


def with_collection_rate(frame):
    # Percentage of invoice value collected, not a customer retention measure.
    return frame.withColumn("collection_rate_pct", F.when(
        F.col("invoiced_gbp") > ZERO,
        F.col("received_gbp") / F.col("invoiced_gbp") * F.lit(100)
    ).cast("decimal(9,2)"))


payment_totals = payments.groupBy("invoice_id").agg(
    F.sum("amount_gbp").cast("decimal(18,2)").alias("received_gbp"),
    F.count("*").alias("payment_count"),
    F.max("payment_date").alias("last_payment_date"),
)
invoice_balances = (
    invoices.select("invoice_id", "customer_id", "issue_date", "due_date",
        F.col("amount_gbp").cast("decimal(18,2)").alias("invoiced_gbp"))
    .join(payment_totals, "invoice_id", "left")
    .join(customers.select("customer_id", "customer_name", "region"), "customer_id", "left")
    .withColumn("received_gbp", F.coalesce(F.col("received_gbp"), ZERO))
    .withColumn("payment_count", F.coalesce(F.col("payment_count"), F.lit(0)).cast("long"))
    .withColumn("outstanding_gbp", F.greatest(F.col("invoiced_gbp") - F.col("received_gbp"), ZERO).cast("decimal(18,2)"))
    .withColumn("credit_gbp", F.greatest(F.col("received_gbp") - F.col("invoiced_gbp"), ZERO).cast("decimal(18,2)"))
    .withColumn("payment_status", F.when(F.col("credit_gbp") > ZERO, "overpaid")
        .when(F.col("outstanding_gbp") == ZERO, "fully_paid")
        .when(F.col("received_gbp") > ZERO, "partially_paid").otherwise("unpaid"))
    .withColumn("overdue_gbp", F.when(F.col("due_date") < AS_OF, F.col("outstanding_gbp")).otherwise(ZERO))
    .withColumn("days_overdue", F.when(F.col("outstanding_gbp") > ZERO,
        F.greatest(F.datediff(AS_OF, F.col("due_date")), F.lit(0))).otherwise(F.lit(0)))
)

customer_totals = invoice_balances.groupBy("customer_id").agg(
    F.count("*").alias("invoice_count"),
    F.sum(F.when(F.col("outstanding_gbp") > ZERO, 1).otherwise(0)).alias("outstanding_invoice_count"),
    *money_sums(),
)
customer_summary = customers.select("customer_id", "customer_name", "region", "joined_date").join(customer_totals, "customer_id", "left")
for name in MONEY_COLUMNS:
    customer_summary = customer_summary.withColumn(name, F.coalesce(F.col(name), ZERO))
for name in ("invoice_count", "outstanding_invoice_count"):
    customer_summary = customer_summary.withColumn(name, F.coalesce(F.col(name), F.lit(0)).cast("long"))

regional_summary = with_collection_rate(customer_summary.groupBy("region").agg(
    F.count("*").alias("customer_count"), F.sum("invoice_count").alias("invoice_count"), *money_sums()
))
monthly_summary = with_collection_rate(invoice_balances.withColumn("invoice_month", F.trunc("issue_date", "month"))
    .groupBy("invoice_month").agg(F.count("*").alias("invoice_count"), *money_sums()))

source_freshness = reduce(lambda left, right: left.unionByName(right),
    [frame.select("_ingested_at") for frame in (customers, invoices, payments)]
).agg(F.max("_ingested_at").alias("last_source_ingested_at"))
validation_freshness = silver["quality_summary"].agg(F.max("evaluated_at").alias("silver_evaluated_at"))
business_overview = with_collection_rate(invoice_balances.agg(
    F.count("*").alias("invoice_count"), *money_sums(),
    *[F.sum(F.when(F.col("payment_status") == status, 1).otherwise(0)).alias(f"{status}_invoices")
        for status in ("fully_paid", "partially_paid", "unpaid", "overpaid")],
).crossJoin(customers.agg(F.count("*").alias("customer_count")))
 .crossJoin(payments.agg(F.count("*").alias("payment_count")))
 .crossJoin(source_freshness).crossJoin(validation_freshness))

# This percentage describes acceptance by the pipeline, not an overall quality score.
data_quality_summary = silver["quality_summary"].withColumn("acceptance_rate_pct",
    (F.col("accepted_rows") / F.col("source_rows") * F.lit(100)).cast("decimal(7,2)"))
data_quality_rules = silver["quality_rules"]
data_quality_exceptions = silver["quarantine"].select(
    "entity", "business_key", "source_record_id", "reason_codes", "raw_record_json",
    "_raw_record_id", "_source_file", "_ingested_at", "quality_run_id", "evaluated_at",
)

# COMMAND ----------

outputs = {name: stamp(frame) for name, frame in {
    "invoice_balances": invoice_balances,
    "customer_summary": customer_summary,
    "regional_summary": regional_summary,
    "monthly_summary": monthly_summary,
    "business_overview": business_overview,
    "data_quality_summary": data_quality_summary,
    "data_quality_rules": data_quality_rules,
    "data_quality_exceptions": data_quality_exceptions,
}.items()}
expected_counts = {
    "invoice_balances": 30000, "customer_summary": 10000, "regional_summary": 4,
    "monthly_summary": 3, "business_overview": 1, "data_quality_summary": 3,
    "data_quality_rules": 5, "data_quality_exceptions": 105,
}


def verify(frames):
    for name, frame in frames.items():
        assert frame.count() == expected_counts[name], f"Unexpected gold count: {name}"
        assert {row["_gold_run_id"] for row in frame.select("_gold_run_id").distinct().collect()} == {GOLD_RUN_ID}
    for name, key in (("invoice_balances", "invoice_id"), ("customer_summary", "customer_id"),
                      ("regional_summary", "region"), ("monthly_summary", "invoice_month")):
        assert frames[name].select(key).distinct().count() == expected_counts[name], f"Duplicate gold key: {name}"
    expected = manifest["expected_clean_metrics"]
    overview = frames["business_overview"].first()
    for name in ("invoice_balances", "customer_summary", "regional_summary", "monthly_summary", "business_overview"):
        total = frames[name].agg(*money_sums()).first()
        for column in ("invoiced_gbp", "received_gbp", "outstanding_gbp"):
            assert total[column] == Decimal(expected[column]), f"{name}: {column} does not reconcile"
        assert total["credit_gbp"] == Decimal("0.00"), f"{name}: unexpected baseline credit"
        assert total["overdue_gbp"] == Decimal(expected["outstanding_gbp"]), f"{name}: overdue baseline differs"
    for column in ("fully_paid_invoices", "partially_paid_invoices", "unpaid_invoices"):
        assert overview[column] == expected[column], f"Unexpected status count: {column}"
    assert overview["overpaid_invoices"] == 0
    assert overview["customer_count"] == 10000 and overview["payment_count"] == 27000
    balances = frames["invoice_balances"]
    invalid = ((F.col("outstanding_gbp") < ZERO) | (F.col("credit_gbp") < ZERO)
        | (F.col("invoiced_gbp") + F.col("credit_gbp") != F.col("received_gbp") + F.col("outstanding_gbp")))
    assert balances.filter(invalid).limit(1).count() == 0, "Invoice-level money conservation failed"
    assert balances.agg(F.sum("payment_count").alias("count")).first()["count"] == 27000
    assert frames["data_quality_summary"].agg(F.sum("excluded_rows").alias("count")).first()["count"] == 105


verify(outputs)
print("All gold checks passed before publication.")
for name, frame in outputs.items():
    target = f"{CATALOG}.gold.{name}"
    frame.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"PUBLISHED {target}")
verify({name: spark.table(f"{CATALOG}.gold.{name}") for name in outputs})
print("Gold publication verified. Ready for the application screens.")

# COMMAND ----------

display(spark.table(f"{CATALOG}.gold.business_overview"))
display(spark.table(f"{CATALOG}.gold.regional_summary").orderBy("region"))
display(spark.table(f"{CATALOG}.gold.monthly_summary").orderBy("invoice_month"))
display(spark.table(f"{CATALOG}.gold.data_quality_rules").orderBy("rule_code"))
display(spark.table(f"{CATALOG}.gold.customer_summary").orderBy(F.desc("outstanding_gbp"), "customer_id").limit(10))