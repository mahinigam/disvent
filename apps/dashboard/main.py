import os
from typing import Any

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


API_URL = os.getenv("API_URL", "http://localhost:8001/api/v1")
API_KEY = os.getenv("DISVENT_API_KEY")


def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    headers = {"X-API-Key": API_KEY} if API_KEY else None
    response = requests.get(f"{API_URL}{path}", params=params, headers=headers, timeout=5)
    response.raise_for_status()
    return response.json()


st.set_page_config(page_title="Disvent", page_icon="D", layout="wide")

st.title("Disvent Real-Time Risk Operations")
st.caption("Live transaction observability backed by Redpanda, Spark, ClickHouse, DuckDB, and FastAPI.")

with st.sidebar:
    st.header("Controls")
    merchant_id = st.text_input("Merchant", value="merchant_1")
    user_id = st.text_input("User", value="user_1")
    limit = st.slider("Rows", min_value=5, max_value=100, value=24, step=1)
    refresh = st.button("Refresh", type="primary", use_container_width=True)

try:
    health = api_get("/health")
except Exception as exc:
    st.error(f"API unavailable: {exc}")
    st.stop()

status = health.get("status", "unknown")
st.info(f"System status: {status}")

try:
    metrics = api_get("/metrics/realtime-throughput")
    metric_cols = st.columns(4)
    metric_cols[0].metric("Transactions", f"{metrics['transactions_total']:,}")
    metric_cols[1].metric("Last minute", f"{metrics['transactions_last_minute']:,}")
    metric_cols[2].metric("Alerts last hour", f"{metrics['anomalies_last_hour']:,}")
    metric_cols[3].metric("Latest event", metrics["latest_event_time"] or "No events")
except Exception as exc:
    st.warning(f"Realtime metrics unavailable: {exc}")

left, right = st.columns([1.15, 0.85], gap="large")

with left:
    st.subheader(f"Merchant Velocity: {merchant_id}")
    try:
        merchant_data = api_get(f"/merchant/{merchant_id}/stats", {"limit": limit})
        if merchant_data:
            merchant_df = pd.DataFrame(merchant_data)
            fig = px.bar(
                merchant_df.sort_values("hour"),
                x="hour",
                y="total_amount",
                color="transaction_count",
                labels={"hour": "Hour", "total_amount": "Total amount", "transaction_count": "Transactions"},
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(merchant_df, use_container_width=True, hide_index=True)
        else:
            st.info("No merchant aggregates yet. Start the generator and let ClickHouse ingest events.")
    except Exception as exc:
        st.warning(f"Merchant analytics unavailable: {exc}")

with right:
    st.subheader(f"Risk Timeline: {user_id}")
    try:
        risk_data = api_get(f"/risk-score/{user_id}", {"limit": limit})
        if risk_data:
            risk_df = pd.DataFrame(risk_data)
            st.dataframe(risk_df, use_container_width=True, hide_index=True)
            fig = px.line(
                risk_df.sort_values("calculated_at"),
                x="calculated_at",
                y="risk_score",
                markers=True,
                labels={"calculated_at": "Calculated", "risk_score": "Risk score"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No risk alerts for this user yet.")
    except Exception as exc:
        st.warning(f"Risk timeline unavailable: {exc}")

st.subheader(f"Historical Audit: {user_id}")
try:
    historical_data = api_get(f"/historical/user/{user_id}")
    if historical_data:
        st.dataframe(pd.DataFrame(historical_data), use_container_width=True, hide_index=True)
    else:
        st.info("No historical Parquet records found for this user.")
except Exception as exc:
    st.warning(f"Historical audit unavailable: {exc}")

if refresh:
    st.rerun()
