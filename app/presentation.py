"""Formatting and filters, with decimal amounts retained for totals/exports."""
from decimal import Decimal
import json
import pandas as pd

RULE_LABELS = {
    "duplicate_customer_id": "Duplicate customer record",
    "invoice_amount_not_positive": "Invoice amount is not positive",
    "invoice_customer_not_found": "Invoice references an unknown customer",
    "payment_amount_not_positive": "Payment amount is not positive",
    "payment_invoice_not_found": "Payment references an unknown invoice",
}


def gbp(value):
    return f"£{Decimal(str(value)):,.2f}"


def reason_codes(value):
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        return decoded if isinstance(decoded, list) else [str(decoded)]
    return list(value) if value is not None else []


def filter_customers(frame, region="All regions", search="", outstanding_only=False):
    result = frame
    if region != "All regions":
        result = result[result["region"] == region]
    if search.strip():
        term = search.strip()
        mask = result["customer_id"].str.contains(term, case=False, regex=False, na=False)
        mask |= result["customer_name"].str.contains(term, case=False, regex=False, na=False)
        result = result[mask]
    if outstanding_only:
        result = result[result["outstanding_gbp"] > 0]
    return result.sort_values(["outstanding_gbp", "customer_id"], ascending=[False, True]).copy()


def filter_exceptions(frame, entity="All entities", rule="All rules"):
    result = frame
    if entity != "All entities":
        result = result[result["entity"] == entity]
    if rule != "All rules":
        result = result[result["reason_codes"].map(lambda value: rule in reason_codes(value))]
    return result.sort_values(["entity", "business_key", "source_record_id"]).copy()


def display_money(frame, columns):
    result = frame.copy()
    for name in columns:
        if name in result:
            result[name] = result[name].map(gbp)
    return result


def chart_money(frame, columns):
    # Convert only display chart values; financial calculations stay decimal.
    result = frame.copy()
    for name in columns:
        result[name] = pd.to_numeric(result[name]).astype(float)
    return result


def csv_bytes(frame):
    return frame.to_csv(index=False).encode("utf-8")
