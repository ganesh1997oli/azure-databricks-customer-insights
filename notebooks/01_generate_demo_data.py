# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — Generate synthetic customer, invoice and payment sources
# MAGIC All records represent a fictional UK utility. No real customer data is used.
# MAGIC The reference date is fixed so the demo and reconciliation totals are reproducible.
# MAGIC This creates CSV source files in a managed Unity Catalog volume; it does not create bronze tables.
# MAGIC Reruns preserve identical files and refuse to replace files with different contents.

# COMMAND ----------

import csv
import hashlib
import io
import json
import random
from datetime import date, timedelta
from pathlib import Path

CATALOG = "customer_insights_dev"
VOLUME = f"{CATALOG}.bronze.landing"
LANDING_ROOT = Path(f"/Volumes/{CATALOG}/bronze/landing/demo_v1")
AS_OF = date(2026, 10, 4)
CUSTOMER_COUNT = 10_000
SEED = 42

# COMMAND ----------

# Managed storage is supplied by the serverless workspace.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME} COMMENT 'Synthetic CSV sources for the customer insights demo'")

# COMMAND ----------

def money(cents):
    """Format integer pennies without floating-point rounding."""
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def generate_dataset():
    rng = random.Random(SEED)
    customers, invoices, payments = [], [], []
    invoice_total, payment_total = 0, 0
    regions = ["London", "Midlands", "North", "South West"]
    # Three complete months before the reference month.
    month_index = AS_OF.year * 12 + AS_OF.month - 1
    months = [divmod(month_index - offset, 12) for offset in (3, 2, 1)]

    for number in range(1, CUSTOMER_COUNT + 1):
        customer_id = f"C{number:06d}"
        customers.append({
            "customer_id": customer_id,
            "customer_name": f"Synthetic Customer {number:06d}",
            "email": f"customer{number:06d}@example.com",
            "region": regions[(number - 1) % len(regions)],
            "joined_date": (AS_OF - timedelta(days=365 + number % 730)).isoformat(),
        })
        for month_number, (year, zero_based_month) in enumerate(months):
            invoice_number = (number - 1) * 3 + month_number + 1
            invoice_id = f"I{invoice_number:07d}"
            issued = date(year, zero_based_month + 1, 1)
            cents = rng.randint(4_000, 20_000)
            invoice_total += cents
            invoices.append({
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "issue_date": issued.isoformat(),
                "due_date": (issued + timedelta(days=20)).isoformat(),
                "amount_gbp": money(cents),
            })
            # 80% paid in full, 10% partially paid, 10% without a payment.
            payment_bucket = (invoice_number - 1) % 10
            if payment_bucket < 9:
                paid = cents if payment_bucket < 8 else cents // 2
                payment_total += paid
                payments.append({
                    "payment_id": f"P{invoice_number:07d}",
                    "invoice_id": invoice_id,
                    "payment_date": (issued + timedelta(days=5 + invoice_number % 25)).isoformat(),
                    "amount_gbp": money(paid),
                })

    # Five deliberately isolated defect scenarios. No payments refer to bad invoices.
    customers.extend(dict(row) for row in customers[:25])
    for number in range(1, 31):
        invoices.append({
            "invoice_id": f"BAD_AMOUNT_I{number:04d}", "customer_id": f"C{number:06d}",
            "issue_date": "2026-09-01", "due_date": "2026-09-21", "amount_gbp": "-5.00",
        })
    for number in range(1, 21):
        invoices.append({
            "invoice_id": f"BAD_CUSTOMER_I{number:04d}", "customer_id": "C999999",
            "issue_date": "2026-09-01", "due_date": "2026-09-21", "amount_gbp": "100.00",
        })
    for number in range(1, 16):
        payments.append({
            "payment_id": f"BAD_AMOUNT_P{number:04d}", "invoice_id": f"I{number:07d}",
            "payment_date": "2026-09-15", "amount_gbp": "-2.00",
        })
        payments.append({
            "payment_id": f"BAD_INVOICE_P{number:04d}", "invoice_id": "I9999999",
            "payment_date": "2026-09-15", "amount_gbp": "25.00",
        })

    datasets = {"customers": customers, "invoices": invoices, "payments": payments}
    # An identifier per source record makes duplicates separately traceable.
    for entity, rows in datasets.items():
        for index, row in enumerate(rows, 1):
            row["source_record_id"] = f"{entity}:{index:08d}"
        rng.shuffle(rows)

    manifest = {
        "dataset_version": "demo_v1", "synthetic": True,
        "as_of_date": AS_OF.isoformat(), "seed": SEED, "currency": "GBP",
        "source_row_counts": {name: len(rows) for name, rows in datasets.items()},
        "expected_clean_row_counts": {"customers": 10000, "invoices": 30000, "payments": 27000},
        "expected_excluded_records": {
            "duplicate_customer_id": 25, "invoice_amount_not_positive": 30,
            "invoice_customer_not_found": 20, "payment_amount_not_positive": 15,
            "payment_invoice_not_found": 15,
        },
        "expected_clean_metrics": {
            "invoiced_gbp": money(invoice_total), "received_gbp": money(payment_total),
            "outstanding_gbp": money(invoice_total - payment_total),
            "fully_paid_invoices": 24000, "partially_paid_invoices": 3000,
            "unpaid_invoices": 3000,
        },
        "notes": [
            "Duplicates are identical business records with distinct source_record_id values; retain one customer per ID.",
            "Outstanding balances are amounts owed, not proof of revenue leakage.",
            "Five source defect scenarios are explicit fixtures; real ingestion must validate all records regardless of ID prefix.",
        ],
    }
    return datasets, manifest


def encode_csv(rows):
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def prepare_files(datasets, manifest):
    files = {f"{name}/batch_001.csv": encode_csv(rows) for name, rows in datasets.items()}
    manifest = dict(manifest)
    manifest["source_sha256"] = {
        name: hashlib.sha256(payload).hexdigest() for name, payload in files.items()
    }
    files["manifest.json"] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return files, manifest


def publish_files(root, files):
    # Check every existing file before writing anything. Changed source fixtures
    # should use a new dataset version rather than silently modifying a batch.
    for relative_path, payload in files.items():
        target = root / relative_path
        if target.exists() and target.read_bytes() != payload:
            raise FileExistsError(f"Different content already exists at {target}. Use a new version directory.")
    for relative_path, payload in files.items():
        target = root / relative_path
        if target.exists():
            print(f"UNCHANGED {target}")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("wb") as handle:
                handle.write(payload)
            print(f"CREATED   {target}")

# COMMAND ----------

datasets, manifest = generate_dataset()
files, manifest = prepare_files(datasets, manifest)
publish_files(LANDING_ROOT, files)
print("\nRaw source row counts:")
for entity, count in manifest["source_row_counts"].items():
    print(f"  {entity}: {count:,}")
print("\nExpected metrics AFTER cleaning; raw totals include deliberate defects:")
print(json.dumps(manifest["expected_clean_metrics"], indent=2))

# COMMAND ----------

# Read the persisted CSV files back using Spark. Keep fields as strings in the
# source preview; typed conversions and quality enforcement come in silver.
for entity, expected_count in manifest["source_row_counts"].items():
    path = str(LANDING_ROOT / entity / "batch_001.csv")
    source_df = spark.read.option("header", "true").option("inferSchema", "false").csv(path)
    observed_count = source_df.count()
    assert observed_count == expected_count, f"{entity}: expected {expected_count}, got {observed_count}"
    print(f"VERIFIED {entity}: {observed_count:,} rows read from the volume")
    display(source_df.limit(5))