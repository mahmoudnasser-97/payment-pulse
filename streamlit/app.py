import os
import time
import redis
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="PaymentPulse Dashboard",
    page_icon="💳",
    layout="wide",
)

# Connection helpers
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

PG_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DB",       "pulse_dw"),
    "user":     os.getenv("PG_USER",     "pulse"),
    "password": os.getenv("PG_PASSWORD", "pulse"),
}

@st.cache_resource
def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                       decode_responses=True)

@st.cache_resource
def get_pg():
    return psycopg2.connect(**PG_CONFIG)

# Data fetchers
def fetch_realtime_metrics(r):
    """Pull live counters from Redis written by the Spark streaming job"""
    total        = int(r.get("pulse:total_transactions") or 0)
    fraud        = int(r.get("pulse:total_fraud")        or 0)
    revenue      = float(r.get("pulse:total_revenue")    or 0)
    tps_raw      = r.lrange("pulse:tps_window", 0, 59)
    tps          = sum(int(x) for x in tps_raw) / max(len(tps_raw), 1)
    fraud_rate   = (fraud / total * 100) if total > 0 else 0

    service_keys = r.hgetall("pulse:by_service")
    gov_keys     = r.hgetall("pulse:by_governorate")

    return {
        "total":        total,
        "fraud":        fraud,
        "revenue":      revenue,
        "tps":          round(tps, 2),
        "fraud_rate":   round(fraud_rate, 2),
        "by_service":   {k: int(v) for k, v in service_keys.items()},
        "by_gov":       {k: int(v) for k, v in gov_keys.items()},
    }

def fetch_historical(conn):
    """Pull daily summaries from PostgreSQL written by Dagster"""
    try:
        merchant_df = pd.read_sql("""
            SELECT summary_date, service_type,
                   SUM(total_transactions) AS total_transactions,
                   SUM(total_amount_egp)   AS total_amount_egp,
                   SUM(fraud_count)        AS fraud_count
            FROM merchant_daily_summary
            GROUP BY summary_date, service_type
            ORDER BY summary_date DESC
            LIMIT 200;
        """, conn)

        gov_df = pd.read_sql("""
            SELECT summary_date, governorate,
                   total_transactions, total_amount_egp, fraud_count
            FROM governorate_daily_summary
            ORDER BY summary_date DESC
            LIMIT 200;
        """, conn)
    except Exception:
        merchant_df = pd.DataFrame()
        gov_df      = pd.DataFrame()

    return merchant_df, gov_df

# Dashboard layout
st.title("PaymentPulse: Live Analytics Dashboard")
st.caption("Real-time Egyptian payment network monitoring")

placeholder = st.empty()

while True:
    try:
        r    = get_redis()
        conn = get_pg()
        m    = fetch_realtime_metrics(r)
        merchant_df, gov_df = fetch_historical(conn)
    except Exception as e:
        st.error(f"Connection error: {e}")
        time.sleep(5)
        continue

    with placeholder.container():

        # Row 1: KPI cards 
        st.subheader("📊 Real-Time KPIs")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Transactions", f"{m['total']:,}")
        k2.metric("Fraud Detected",     f"{m['fraud']:,}")
        k3.metric("Fraud Rate",         f"{m['fraud_rate']}%")
        k4.metric("Avg TPS",            f"{m['tps']}")
        k5.metric("Total Revenue",      f"EGP {m['revenue']:,.0f}")

        st.divider()

        # Row 2: Live charts
        st.subheader("Live Transaction Breakdown")
        c1, c2 = st.columns(2)

        with c1:
            if m["by_service"]:
                svc_df = pd.DataFrame(
                    m["by_service"].items(),
                    columns=["Service Type", "Count"]
                ).sort_values("Count", ascending=True)
                fig = px.bar(
                    svc_df, x="Count", y="Service Type",
                    orientation="h",
                    title="Transactions by Service Type",
                    color="Count",
                    color_continuous_scale="teal",
                )
                fig.update_layout(showlegend=False,
                                  coloraxis_showscale=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Waiting for service type data from Kafka")

        with c2:
            if m["by_gov"]:
                gov_live_df = pd.DataFrame(
                    m["by_gov"].items(),
                    columns=["Governorate", "Count"]
                ).sort_values("Count", ascending=False)
                fig2 = px.bar(
                    gov_live_df, x="Governorate", y="Count",
                    title="Transactions by Governorate",
                    color="Count",
                    color_continuous_scale="blues",
                )
                fig2.update_layout(xaxis_tickangle=-45,
                                   coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Waiting for governorate data from Kafka")

        st.divider()

        # Row 3: Historical summaries from PostgreSQL
        st.subheader("Historical Daily Summaries (from Dagster)")
        h1, h2 = st.columns(2)

        with h1:
            if not merchant_df.empty:
                fig3 = px.line(
                    merchant_df,
                    x="summary_date", y="total_amount_egp",
                    color="service_type",
                    title="Daily Revenue by Service Type",
                    labels={"total_amount_egp": "Revenue (EGP)",
                            "summary_date": "Date"},
                )
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.info("No historical data yet "
                        "Dagster runs summaries at midnight")

        with h2:
            if not gov_df.empty:
                fig4 = px.bar(
                    gov_df,
                    x="governorate", y="fraud_count",
                    title="Fraud Count by Governorate (Historical)",
                    color="fraud_count",
                    color_continuous_scale="reds",
                )
                fig4.update_layout(xaxis_tickangle=-45,
                                   coloraxis_showscale=False)
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("No historical data yet "
                        "Dagster runs summaries at midnight")

    # Refresh every 5 seconds
    time.sleep(5)