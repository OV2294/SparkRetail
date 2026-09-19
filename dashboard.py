"""
SparkRetail - Streamlit Dashboard
==================================
A lightweight presentation layer on top of the Spark pipeline: Spark does
the heavy processing (Modules 1-4), this dashboard reads the RESULTS with
pandas and visualizes them. It does not run Spark itself -- keeping the
processing engine and the presentation layer separate is a deliberate
architecture choice, not a shortcut.

Two tabs:
  1. Batch Analytics  - reads data/sales.csv directly (the same source
                         Modules 1-3 process) and shows regional revenue,
                         top products, monthly trend, payment mix.
  2. Live Streaming    - polls output/streaming_snapshot.csv, which
                         Module 4 writes to on every micro-batch while it's
                         running. Run module4_streaming.py in one terminal,
                         then open this tab in another to watch windows
                         fill up in near real time.

Run:
    pip install streamlit pandas
    streamlit run dashboard.py
"""

import os
import time

import pandas as pd
import streamlit as st

HERE = os.path.dirname(os.path.abspath(__file__))
SALES_CSV = os.path.join(HERE, "data", "sales.csv")
SNAPSHOT_CSV = os.path.join(HERE, "output", "streaming_snapshot.csv")

st.set_page_config(page_title="SparkRetail Dashboard", layout="wide")
st.title("SparkRetail Dashboard")
st.caption("Spark processes the data (Modules 1-4) -- this dashboard just visualizes the results.")

tab_batch, tab_stream = st.tabs(["Batch Analytics", "Live Streaming"])

# ======================================================================
# TAB 1: Batch Analytics
# ======================================================================
with tab_batch:
    if not os.path.exists(SALES_CSV):
        st.error(f"Dataset not found at {SALES_CSV}. Run `python generate_data.py` first.")
    else:
        @st.cache_data
        def load_sales(path, mtime):
            # mtime is part of the cache key so regenerating the dataset
            # invalidates the cache automatically.
            df = pd.read_csv(path, parse_dates=["order_date"])
            df["revenue"] = (df["unit_price"] * df["quantity"]
                              * (1 - df["discount_pct"] / 100)).round(2)
            df["month"] = df["order_date"].dt.to_period("M").astype(str)
            return df

        df = load_sales(SALES_CSV, os.path.getmtime(SALES_CSV))

        regions = sorted(df["region"].unique())
        selected_regions = st.multiselect("Filter by region", regions, default=regions)
        view = df[df["region"].isin(selected_regions)]

        # ---- KPI row ----
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Revenue", f"Rs {view['revenue'].sum():,.0f}")
        c2.metric("Orders", f"{len(view):,}")
        c3.metric("Avg Order Value", f"Rs {view['revenue'].mean():,.2f}")
        c4.metric("Unique Customers", f"{view['customer_id'].nunique():,}")

        st.divider()

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Revenue by Region")
            region_rev = (view.groupby("region")["revenue"].sum()
                              .sort_values(ascending=False))
            st.bar_chart(region_rev)

        with col2:
            st.subheader("Top 10 Products by Revenue")
            product_rev = (view.groupby("product_name")["revenue"].sum()
                               .sort_values(ascending=False).head(10))
            st.bar_chart(product_rev)

        col3, col4 = st.columns(2)

        with col3:
            st.subheader("Monthly Revenue Trend")
            monthly = view.groupby("month")["revenue"].sum().sort_index()
            st.line_chart(monthly)

        with col4:
            st.subheader("Payment Method Mix")
            payment_mix = view["payment_method"].value_counts()
            st.bar_chart(payment_mix)

        with st.expander("Show raw sample (first 100 rows of current filter)"):
            st.dataframe(view.head(100), use_container_width=True)

# ======================================================================
# TAB 2: Live Streaming
# ======================================================================
with tab_stream:
    st.subheader("Live windowed sales (from Module 4)")
    st.caption(
        "Run `python modules/module4_streaming.py` in a separate terminal "
        "while this tab is open. It writes a snapshot to "
        "output/streaming_snapshot.csv on every micro-batch."
    )

    auto_refresh = st.checkbox("Auto-refresh every 5 seconds", value=True)
    refresh_clicked = st.button("Refresh now")

    placeholder = st.empty()

    def render_snapshot():
        with placeholder.container():
            if not os.path.exists(SNAPSHOT_CSV):
                st.info(
                    "No snapshot yet. Start module4_streaming.py -- the "
                    "first snapshot appears after its first micro-batch "
                    "(a few seconds)."
                )
                return

            snap = pd.read_csv(SNAPSHOT_CSV)
            if snap.empty:
                st.info("Snapshot file exists but has no rows yet.")
                return

            last_updated = snap["last_updated"].iloc[0]
            batch_id = snap["batch_id"].iloc[0]
            st.write(f"**Last updated:** {last_updated}  |  **Batch:** {batch_id}")

            latest_window = snap[["win_start", "win_end"]].drop_duplicates().iloc[-1]
            current = snap[(snap["win_start"] == latest_window["win_start"]) &
                           (snap["win_end"] == latest_window["win_end"])]

            c1, c2 = st.columns(2)
            c1.metric("Orders in latest window",
                      f"{current['orders'].sum():,}")
            c2.metric("Revenue in latest window",
                      f"Rs {current['revenue'].sum():,.2f}")

            st.write(f"Latest window: {latest_window['win_start']} - {latest_window['win_end']}")
            st.bar_chart(current.set_index("region")["revenue"])

            st.write("All windows so far:")
            st.dataframe(
                snap.sort_values(["win_start", "revenue"], ascending=[True, False]),
                use_container_width=True,
            )

    render_snapshot()

    if refresh_clicked:
        st.rerun()

    if auto_refresh:
        time.sleep(5)
        st.rerun()
