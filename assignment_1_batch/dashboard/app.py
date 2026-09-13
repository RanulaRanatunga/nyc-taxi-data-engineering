import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from assignment_1_batch.etl.config import DatabaseConfig, get_db_connection
from assignment_1_batch.etl.queries import load_named_queries
from common.db import fetch_df

st.set_page_config(page_title="NYC Taxi Batch Analytics", layout="wide")

QUERIES = load_named_queries()


@st.cache_data(ttl=300, show_spinner=False)
def run_query(name: str) -> pd.DataFrame:
    conn, _ = get_db_connection(DatabaseConfig.from_env(), read_only=True)
    try:
        return fetch_df(conn, QUERIES[name])
    finally:
        conn.close()


def engine_label() -> str:
    cfg = DatabaseConfig.from_env()
    return "PostgreSQL" if cfg.engine_type == "postgres" else "DuckDB"


def main() -> None:
    st.title("NYC Yellow Taxi - Batch Analytics")

    with st.sidebar:
        st.caption(f"Configured engine: **{engine_label()}**")
        if st.button("Refresh data"):
            st.cache_data.clear()
            st.rerun()
        st.caption("Queries: assignment_1_batch/sql/analytical_queries/")

    try:
        kpi = run_query("kpi_summary").iloc[0]
    except Exception as exc:
        st.error(f"Could not query the warehouse: {exc}")
        st.code("python assignment_1_batch/scripts/init_db.py\n"
                "python assignment_1_batch/etl/pipeline.py --months 2023-01 2023-02")
        st.stop()

    if int(kpi["total_trips"]) == 0:
        st.warning("fact_taxi_trips is empty. Run the ETL pipeline first.")
        st.stop()

    fare_per_mile = run_query("avg_fare_per_mile").iloc[0]
    st.caption(f"Pickups from {kpi['first_pickup']} to {kpi['last_pickup']}")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Trips", f"{int(kpi['total_trips']):,}")
    k2.metric("Total revenue", f"${float(kpi['total_revenue']):,.0f}")
    k3.metric("Avg fare per mile", f"${float(fare_per_mile['weighted_fare_per_mile']):.2f}",
              help="SUM(fare_amount) / SUM(trip_distance). "
                   f"Simple average of per-trip fare/mile: ${float(fare_per_mile['simple_avg_fare_per_mile']):.2f}")
    k4.metric("Avg fare per trip", f"${float(kpi['avg_fare']):.2f}")

    st.divider()
    left, right = st.columns(2)

    with left:
        st.subheader("1. Average fare per mile by pickup borough")
        df_borough = run_query("avg_fare_per_mile_by_borough")
        fig = px.bar(df_borough, x="pickup_borough", y="weighted_fare_per_mile", text="weighted_fare_per_mile",
                     hover_data=["total_trips", "total_miles"],
                     labels={"pickup_borough": "Pickup borough", "weighted_fare_per_mile": "Fare per mile ($)"})
        fig.update_traces(texttemplate="$%{text:.2f}", textposition="outside")
        fig.update_layout(height=380, margin=dict(t=20, b=10))
        st.plotly_chart(fig, width="stretch")

    with right:
        st.subheader("2. Peak hours for taxi rides")
        df_hours = run_query("peak_hours")
        fig = px.bar(df_hours, x="pickup_hour", y="trip_count", color="demand_level",
                     category_orders={"demand_level": ["Peak", "Moderate", "Off-peak"]},
                     color_discrete_map={"Peak": "#d62728", "Moderate": "#1f77b4", "Off-peak": "#9ecae1"},
                     hover_data=["pct_of_trips", "total_revenue"],
                     labels={"pickup_hour": "Pickup hour", "trip_count": "Trips", "demand_level": "Demand"})
        fig.update_layout(height=380, margin=dict(t=20, b=10), xaxis=dict(dtick=1))
        st.plotly_chart(fig, width="stretch")
        peak = df_hours.sort_values("volume_rank").head(3)["pickup_hour"].astype(int).tolist()
        st.caption("Top 3 hours: " + ", ".join(f"{h:02d}:00" for h in peak))

    df_heat = run_query("peak_hours_by_weekday")
    heat = df_heat.pivot_table(index=["day_of_week", "day_name"], columns="pickup_hour", values="avg_trips")
    heat.index = heat.index.get_level_values("day_name")
    fig = px.imshow(heat, aspect="auto", color_continuous_scale="Blues",
                    labels={"x": "Pickup hour", "y": "", "color": "Avg trips"})
    fig.update_layout(height=320, margin=dict(t=10, b=10))
    st.markdown("**Average trips per hour by weekday**")
    st.plotly_chart(fig, width="stretch")

    st.subheader("3. Total revenue by payment type")
    df_pay = run_query("revenue_by_payment_type")
    pay_left, pay_right = st.columns([1, 1])
    with pay_left:
        fig = px.bar(df_pay, x="payment_name", y="total_revenue", text="total_revenue",
                     labels={"payment_name": "Payment type", "total_revenue": "Revenue ($)"})
        fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig.update_layout(height=360, margin=dict(t=20, b=10))
        st.plotly_chart(fig, width="stretch")
    with pay_right:
        st.dataframe(df_pay.drop(columns=["payment_type_id"]), hide_index=True, width="stretch")

    st.subheader("Pipeline runs (etl_audit_log)")
    st.dataframe(run_query("recent_etl_runs"), hide_index=True, width="stretch")


main()
