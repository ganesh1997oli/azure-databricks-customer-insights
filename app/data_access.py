"""Read-only Databricks access. No credentials or SQL come from UI input."""
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import re
from urllib.parse import urlsplit

import pandas as pd

CATALOG = "customer_insights_dev"
SCHEMA = "gold"
TABLES = (
    "business_overview", "regional_summary", "monthly_summary", "customer_summary",
    "data_quality_summary", "data_quality_rules", "data_quality_exceptions",
)


class ConfigurationError(RuntimeError):
    pass


class SnapshotMismatch(RuntimeError):
    pass


@dataclass(frozen=True)
class Snapshot:
    frames: dict
    gold_run_id: str
    silver_run_id: str
    as_of_date: object
    loaded_at: datetime


def connect():
    from databricks import sql
    from databricks.sdk.core import Config

    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", warehouse_id):
        raise ConfigurationError("Add the SQL warehouse resource with key sql-warehouse.")
    cfg = Config()
    hostname = urlsplit(cfg.host if "://" in cfg.host else f"https://{cfg.host}").hostname
    if not hostname:
        raise ConfigurationError("The Databricks workspace host is missing.")
    # In Databricks Apps, Config detects the app's managed OAuth credentials.
    return sql.connect(
        server_hostname=hostname,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )


def query(connection, statement, parameters=None):
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters=parameters)
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame.from_records([tuple(row) for row in cursor.fetchall()], columns=columns)


def validate_snapshot(frames):
    runs, silver_runs, dates = set(), set(), set()
    for name in TABLES:
        frame = frames[name]
        if frame.empty:
            raise SnapshotMismatch(f"Missing published rows: {name}.")
        for column, values in (("_gold_run_id", runs), ("_silver_run_id", silver_runs), ("as_of_date", dates)):
            if frame[column].isna().any():
                raise SnapshotMismatch(f"Missing publication metadata: {name}.")
            values.update(frame[column].unique())
    if len(runs) != 1 or len(silver_runs) != 1 or len(dates) != 1:
        raise SnapshotMismatch("Gold tables have different publication versions.")
    if len(frames["business_overview"]) != 1:
        raise SnapshotMismatch("Expected one business overview row.")
    return next(iter(runs)), next(iter(silver_runs)), next(iter(dates))


def read_snapshot():
    with connect() as connection:
        frames = {name: query(connection, f"SELECT * FROM {CATALOG}.{SCHEMA}.{name}") for name in TABLES}
        gold_run_id, silver_run_id, as_of_date = validate_snapshot(frames)
        # Invoice details are loaded only when a user opens one customer.
        invoice_runs = query(connection,
            f"SELECT DISTINCT _gold_run_id FROM {CATALOG}.{SCHEMA}.invoice_balances")
        if set(invoice_runs["_gold_run_id"]) != {gold_run_id}:
            raise SnapshotMismatch("Invoice details belong to a different publication.")
    return Snapshot(frames, gold_run_id, silver_run_id, as_of_date, datetime.now(timezone.utc))


def read_customer_invoices(customer_id, gold_run_id):
    with connect() as connection:
        versions = query(connection, f"SELECT DISTINCT _gold_run_id FROM {CATALOG}.{SCHEMA}.invoice_balances")
        if set(versions["_gold_run_id"]) != {gold_run_id}:
            raise SnapshotMismatch("The pipeline has published a new version. Refresh the app.")
        # Native named parameters keep user-supplied values out of SQL text.
        frame = query(connection, f"""
            SELECT invoice_id, issue_date, due_date, invoiced_gbp, received_gbp,
                   outstanding_gbp, overdue_gbp, credit_gbp, payment_status, days_overdue, _gold_run_id
            FROM {CATALOG}.{SCHEMA}.invoice_balances
            WHERE customer_id = :customer_id AND _gold_run_id = :gold_run_id
            ORDER BY issue_date DESC, invoice_id
        """, {"customer_id": customer_id, "gold_run_id": gold_run_id})
        if frame.empty:
            # Distinguish a valid customer without invoices from concurrent publication.
            versions = query(connection, f"SELECT DISTINCT _gold_run_id FROM {CATALOG}.{SCHEMA}.invoice_balances")
            if set(versions["_gold_run_id"]) != {gold_run_id}:
                raise SnapshotMismatch("Invoice publication changed during the request.")
        return frame
