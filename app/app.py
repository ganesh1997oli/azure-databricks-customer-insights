"""Customer Insights: two read-only application screens backed by gold tables."""
import json
import logging
from decimal import Decimal

import altair as alt
import pandas as pd
import streamlit as st

from data_access import ConfigurationError, SnapshotMismatch, read_customer_invoices, read_snapshot
from presentation import (
    RULE_LABELS, chart_money, csv_bytes, display_money, filter_customers,
    filter_exceptions, gbp, reason_codes,
)

st.set_page_config(page_title="Customer Insights", page_icon="📊", layout="wide")
st.markdown("""<style>
    .block-container {max-width: 1440px; padding-top: 2rem;}
    [data-testid="stMetric"] {background: #f3f7f8; border: 1px solid #dbe5e8;
        border-radius: 12px; padding: 16px;}
    [data-testid="stMetricValue"] {font-size: 1.75rem; color: #123c49;}
    h1, h2, h3 {color: #123c49;}
</style>""", unsafe_allow_html=True)


@st.cache_data(ttl=300, show_spinner=False, max_entries=1)
def load_snapshot():
    # App authorization: all allowed app viewers see the same synthetic dataset.
    return read_snapshot()


@st.cache_data(ttl=300, show_spinner=False, max_entries=100)
def load_invoices(customer_id, gold_run_id):
    return read_customer_invoices(customer_id, gold_run_id)


def show_error(error):
    logging.warning("Customer Insights data load failed (%s): %s", type(error).__name__, str(error) if type(error).__name__ == "ServerOperationError" else "")
    if isinstance(error, SnapshotMismatch):
        st.warning("A data update is in progress. Wait for the pipeline to finish, then select Refresh data.")
    elif isinstance(error, ConfigurationError):
        st.error("The app's data connection needs configuration. Contact the app owner.")
    else:
        st.error("Data is temporarily unavailable. Select Refresh data to try again. If it continues, contact the app owner.")


def show_business(snapshot):
    frames = snapshot.frames
    overview = frames["business_overview"].iloc[0]
    st.title("Business overview")
    st.caption("Portfolio totals • GBP • synthetic UK utilities dataset")
    for column, (label, field) in zip(st.columns(4), (
        ("Invoiced", "invoiced_gbp"), ("Payments received", "received_gbp"),
        ("Outstanding", "outstanding_gbp"), ("Overdue", "overdue_gbp"),
    )):
        column.metric(label, gbp(overview[field]))
    for column, label, value in zip(st.columns(3), ("Customers", "Invoices", "Collection rate"), (
        f"{int(overview['customer_count']):,}", f"{int(overview['invoice_count']):,}",
        f"{overview['collection_rate_pct']:.2f}%",
    )):
        column.metric(label, value)
    st.caption("Outstanding is money owed. Overdue means a positive balance with a due date before the reference date.")

    left, right = st.columns(2)
    with left:
        st.subheader("Outstanding by region")
        region = chart_money(frames["regional_summary"][["region", "outstanding_gbp"]], ["outstanding_gbp"])
        chart = alt.Chart(region).mark_bar(color="#137d87", cornerRadiusEnd=5).encode(
            x=alt.X("outstanding_gbp:Q", title="Outstanding (£)", axis=alt.Axis(format=",.0f")),
            y=alt.Y("region:N", title=None, sort="-x"),
            tooltip=["region:N", alt.Tooltip("outstanding_gbp:Q", title="Outstanding (£)", format=",.2f")],
        ).properties(height=230)
        st.altair_chart(chart, use_container_width=True)
    with right:
        st.subheader("Collection by invoice month")
        monthly = chart_money(frames["monthly_summary"][["invoice_month", "invoiced_gbp", "received_gbp"]], ["invoiced_gbp", "received_gbp"])
        monthly = monthly.sort_values("invoice_month")
        # Categorical month labels prevent timezone shifts and repeated date ticks.
        monthly["invoice_month"] = pd.to_datetime(monthly["invoice_month"]).dt.strftime("%b %Y")
        month_order = monthly["invoice_month"].tolist()
        monthly = monthly.rename(columns={"invoiced_gbp": "Invoiced", "received_gbp": "Received"}).melt(
            id_vars="invoice_month", var_name="Measure", value_name="GBP")
        chart = alt.Chart(monthly).mark_line(point=True).encode(
            x=alt.X("invoice_month:O", title=None, sort=month_order, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("GBP:Q", title="Amount (£)", axis=alt.Axis(format=",.0f")),
            color=alt.Color("Measure:N", scale=alt.Scale(domain=["Invoiced", "Received"], range=["#123c49", "#137d87"])),
            tooltip=[alt.Tooltip("invoice_month:O", title="Invoice month"), "Measure:N", alt.Tooltip("GBP:Q", format=",.2f")],
        ).properties(height=230)
        st.altair_chart(chart, use_container_width=True)
        st.caption("Payments are allocated to the invoice's issue month; this is not a cash-flow chart.")

    st.subheader("Customer explorer")
    st.caption("Filters below apply to this customer list. Portfolio totals and charts above cover all customers.")
    region_column, search_column, toggle_column = st.columns([1, 2, 1])
    region_filter = region_column.selectbox("Region", ["All regions"] + sorted(frames["customer_summary"]["region"].unique()))
    search = search_column.text_input("Customer ID or name", placeholder="For example, C000003")
    outstanding_only = toggle_column.checkbox("Outstanding balances only", value=True)
    filtered = filter_customers(frames["customer_summary"], region_filter, search, outstanding_only)
    fields = ["customer_id", "customer_name", "region", "invoice_count", "invoiced_gbp", "received_gbp", "outstanding_gbp", "overdue_gbp"]
    st.caption(f"{len(filtered):,} matching customers • showing up to 200, highest balance first")
    st.dataframe(display_money(filtered[fields].head(200), ["invoiced_gbp", "received_gbp", "outstanding_gbp", "overdue_gbp"])
        .rename(columns={name: name.replace("_gbp", " (£)").replace("_", " ").title() for name in fields}),
        hide_index=True, width="stretch")
    st.download_button("Download all matching customers", csv_bytes(filtered[fields]),
        "customers.csv", "text/csv", key="download_customers")
    if filtered.empty:
        st.info("No customers match these filters. Clear the search or choose another region.")
        return
    # Bound the selector; a customer can also be found using the search field.
    choices = ["Choose a customer"] + filtered["customer_id"].head(200).tolist()
    selected = st.selectbox("Open invoice details", choices)
    if selected != choices[0]:
        try:
            with st.spinner("Loading invoices…"):
                details = load_invoices(selected, snapshot.gold_run_id)
            if details.empty:
                st.info("This customer has no invoices in the current snapshot.")
            else:
                st.dataframe(display_money(details.drop(columns="_gold_run_id"),
                    ["invoiced_gbp", "received_gbp", "outstanding_gbp", "overdue_gbp", "credit_gbp"]),
                    hide_index=True, width="stretch")
        except Exception as error:
            show_error(error)


def show_quality(snapshot):
    frames = snapshot.frames
    quality = frames["data_quality_summary"]
    source = int(quality["source_rows"].sum())
    accepted = int(quality["accepted_rows"].sum())
    excluded = int(quality["excluded_rows"].sum())
    st.title("Data quality")
    st.caption("Record acceptance, rule failures and traceable exceptions")
    for column, label, value in zip(st.columns(4),
        ("Source records", "Accepted records", "Excluded records", "Acceptance rate"),
        (f"{source:,}", f"{accepted:,}", f"{excluded:,}", f"{Decimal(accepted) / Decimal(source) * 100:.2f}%")):
        column.metric(label, value)
    st.caption("Acceptance rate is the share of records admitted by the pipeline. It is not an overall quality score.")
    st.subheader("Acceptance by source")
    st.dataframe(quality[["entity", "source_rows", "accepted_rows", "excluded_rows", "acceptance_rate_pct"]]
        .sort_values("entity").rename(columns={"entity": "Source", "source_rows": "Source records",
            "accepted_rows": "Accepted", "excluded_rows": "Excluded", "acceptance_rate_pct": "Acceptance (%)"}),
        hide_index=True, width="stretch")

    rules = frames["data_quality_rules"][["rule_code", "excluded_records"]].copy()
    rules["Rule"] = rules["rule_code"].map(lambda code: RULE_LABELS.get(code, code.replace("_", " ").capitalize()))
    st.subheader("Validation failures")
    chart = alt.Chart(rules).mark_bar(color="#cc7951", cornerRadiusEnd=5).encode(
        x=alt.X("excluded_records:Q", title="Affected records", axis=alt.Axis(tickMinStep=1, tickCount=6)),
        y=alt.Y("Rule:N", title=None, sort="-x", axis=alt.Axis(labelLimit=340)), tooltip=["Rule:N", "excluded_records:Q"],
    ).properties(height=240)
    st.altair_chart(chart, use_container_width=True)
    st.caption("One record may violate multiple rules. The excluded-record metric counts each record once.")

    st.subheader("Exception explorer")
    entity_column, rule_column = st.columns(2)
    entity = entity_column.selectbox("Source", ["All entities"] + sorted(frames["data_quality_exceptions"]["entity"].unique()))
    rule = rule_column.selectbox("Validation rule", ["All rules"] + sorted(rules["rule_code"].tolist()),
        format_func=lambda code: RULE_LABELS.get(code, code))
    filtered = filter_exceptions(frames["data_quality_exceptions"], entity, rule)
    st.caption(f"{len(filtered):,} matching exceptions")
    visible = filtered[["entity", "business_key", "source_record_id", "reason_codes"]].copy()
    visible["reason_codes"] = visible["reason_codes"].map(lambda value: "; ".join(RULE_LABELS.get(code, code) for code in reason_codes(value)))
    st.dataframe(visible.rename(columns={"entity": "Source", "business_key": "Business key",
        "source_record_id": "Source record", "reason_codes": "Reasons"}), hide_index=True, width="stretch")
    st.download_button("Download matching exceptions", csv_bytes(visible), "exceptions.csv", "text/csv", key="download_exceptions")
    if filtered.empty:
        st.info("No exceptions match this source and rule. Change either filter to continue.")
        return
    records = filtered["source_record_id"].tolist()
    selected = st.selectbox("Inspect original record", ["Choose a record"] + records)
    if selected != "Choose a record":
        row = filtered[filtered["source_record_id"] == selected].iloc[0]
        st.caption(f"Business key: {row['business_key']} • Source: {row['entity']}")
        try:
            st.json(json.loads(row["raw_record_json"]))
        except (ValueError, TypeError):
            st.code(str(row["raw_record_json"]), language="text")
        st.caption(f"Source file: {row['_source_file']}")


with st.sidebar:
    st.title("Customer Insights")
    st.caption("Utilities portfolio project")
    page = st.radio("Screen", ["Business Overview", "Data Quality"], label_visibility="collapsed")
    if st.button("Refresh data", width="stretch"):
        load_snapshot.clear()
        load_invoices.clear()
    st.divider()
    st.caption("Synthetic customer data")

try:
    with st.spinner("Loading the latest published data…"):
        snapshot = load_snapshot()
except Exception as error:
    show_error(error)
    st.stop()

with st.sidebar:
    st.caption(f"Reference date: {snapshot.as_of_date:%d %b %Y}")
    st.caption(f"Loaded: {snapshot.loaded_at:%d %b %Y, %H:%M} UTC")
    st.caption("Data is cached for up to 5 minutes. Refresh after a pipeline run.")
    with st.expander("Publication details"):
        st.text(f"Gold run: {snapshot.gold_run_id}")
        st.text(f"Silver run: {snapshot.silver_run_id}")
        overview = snapshot.frames["business_overview"].iloc[0]
        st.caption(f"Source ingestion: {overview['last_source_ingested_at']}")
        st.caption(f"Gold built: {overview['_built_at']}")

if page == "Business Overview":
    show_business(snapshot)
else:
    show_quality(snapshot)
