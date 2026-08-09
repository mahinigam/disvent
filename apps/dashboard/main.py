import streamlit as st
import pandas as pd
import requests
import plotly.express as px

API_URL = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Disvent Dashboard", layout="wide")

st.title("💸 Real-Time Fraud & Anomaly Dashboard")
st.markdown("Monitor real-time merchant aggregates and historical user data through our dual-routing API Engine.")

merchant_id_input = st.sidebar.text_input("Merchant ID for Live Stats", value="merchant_1")
user_id_input = st.sidebar.text_input("User ID for Historical Audit", value="user_1")

col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Live Hourly Stats for {merchant_id_input} (ClickHouse)")
    if st.button("Refresh Live Data"):
        with st.spinner("Fetching from API..."):
            try:
                response = requests.get(f"{API_URL}/merchant/{merchant_id_input}/stats")
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        df = pd.DataFrame(data)
                        fig = px.bar(df, x="hour", y="total_amount", title="Total Amount per Hour")
                        st.plotly_chart(fig, use_container_width=True)
                        st.dataframe(df)
                    else:
                        st.info("No data found for this merchant.")
                else:
                    st.error(f"Error: {response.text}")
            except Exception as e:
                st.error(f"Failed to connect to API: {e}")

with col2:
    st.subheader(f"Historical Audit for {user_id_input} (DuckDB)")
    if st.button("Fetch Historical Audit"):
        with st.spinner("Querying Parquet files..."):
            try:
                response = requests.get(f"{API_URL}/historical/user/{user_id_input}")
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        st.dataframe(pd.DataFrame(data))
                    else:
                        st.info("No historical data found.")
                else:
                    st.error(f"Error: {response.text}")
            except Exception as e:
                st.error(f"Failed to connect to API: {e}")
