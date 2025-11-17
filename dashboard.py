import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ------------- PAGE SETUP -------------------
st.set_page_config(page_title="Real-Time Delivery Dashboard", layout="wide")
st.title("🚚 Real-Time Food Delivery Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5433/kafka_db"


# ------------- DATABASE ENGINE -------------------
@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


engine = get_engine()


# ------------- LOAD DATA FUNCTION -------------------
def load_data(cuisine_filter=None, region_filter=None, limit=500):
    query = "SELECT * FROM deliveries ORDER BY timestamp DESC LIMIT :limit"
    params = {"limit": limit}

    if cuisine_filter != "All" or region_filter != "All":
        query = "SELECT * FROM deliveries WHERE 1=1"
        if cuisine_filter != "All":
            query += " AND cuisine = :cuisine"
            params["cuisine"] = cuisine_filter
        if region_filter != "All":
            query += " AND region = :region"
            params["region"] = region_filter

        query += " ORDER BY timestamp DESC LIMIT :limit"

    try:
        df = pd.read_sql(text(query), engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


# ---------------- SIDEBAR --------------------
st.sidebar.header("Filters & Settings")

refresh_interval = st.sidebar.slider("Auto-refresh interval (seconds)", 5, 30, 10)

limit_records = st.sidebar.number_input(
    "Number of latest deliveries to load", 100, 5000, 500, step=100
)

cuisine_filter = st.sidebar.selectbox(
    "Filter by cuisine",
    [
        "All",
        "Burgers",
        "BBQ",
        "Sushi",
        "Chinese",
        "Pizza",
        "Indian",
        "Mexican",
        "Vegan",
    ],
)

region_filter = st.sidebar.selectbox(
    "Filter by region",
    ["All", "Downtown", "Uptown", "Suburbs", "Airport", "Campus", "Waterfront"],
)

st.sidebar.caption("This dashboard auto-refreshes based on the interval above.")

placeholder = st.empty()

# ---------------- MAIN LOOP ---------------------
while True:
    df = load_data(cuisine_filter, region_filter, limit=limit_records)

    with placeholder.container():
        if df.empty:
            st.warning("Waiting for streaming data...")
            time.sleep(refresh_interval)
            continue

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # --------- KPIs ---------
        colA, colB, colC, colD = st.columns(4)
        colA.metric("Total Deliveries", len(df))
        colB.metric(
            "Avg Delivery Time (min)", round(df["delivery_time_estimate_min"].mean(), 2)
        )
        colC.metric("Avg Surge Multiplier", round(df["surge_multiplier"].mean(), 2))
        colD.metric("Avg Driver Rating", round(df["driver_rating"].mean(), 2))

        st.markdown("---")

        # ==============================
        #        AGGREGATIONS
        # ==============================
        cuisine_counts = df.groupby("cuisine").size().reset_index(name="num_deliveries")
        region_delivery_time = (
            df.groupby("region")["delivery_time_estimate_min"].mean().reset_index()
        )

        # ==============================
        #            CHARTS
        # ==============================

        # ---- Chart 1: Horizontal bar (cuisine) ----
        fig_cuisine = px.bar(
            cuisine_counts,
            y="cuisine",
            x="num_deliveries",
            orientation="h",
            color="num_deliveries",
            title="Deliveries by Cuisine (Horizontal Bar)",
        )

        # ---- Chart 2: Line chart (region times) ----
        fig_region_time = px.line(
            region_delivery_time,
            x="region",
            y="delivery_time_estimate_min",
            markers=True,
            title="Avg Delivery Time by Region (Line Chart)",
        )

        col1, col2 = st.columns(2)
        col1.plotly_chart(fig_cuisine, use_container_width=True)
        col2.plotly_chart(fig_region_time, use_container_width=True)

        st.markdown("## Surge & Ratings")

        # ---- Chart 3: Boxplot (surge) ----
        fig_surge = px.box(
            df,
            y="surge_multiplier",
            title="Surge Multiplier Distribution (Boxplot)",
        )

        # ---- Chart 4: KDE (ratings) ----
        fig_rating = px.histogram(
            df,
            x="driver_rating",
            nbins=30,
            histnorm="probability density",
            marginal="box",
            title="Driver Rating Distribution (Density)",
        )

        col3, col4 = st.columns(2)
        col3.plotly_chart(fig_surge, use_container_width=True)
        col4.plotly_chart(fig_rating, use_container_width=True)

        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    time.sleep(refresh_interval)
