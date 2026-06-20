import os
import sys
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# Add project root to Python path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.base import sqlalchemy_url

# Override host to localhost for local Streamlit execution to avoid pg_hba.conf issues
if "172.18.0.1" in sqlalchemy_url:
    sqlalchemy_url = sqlalchemy_url.replace("172.18.0.1", "localhost")

# ------------------------------------------------------------------ #
# Page Configuration
# ------------------------------------------------------------------ #
st.set_page_config(page_title="Data Analytics Dashboard", layout="wide")
st.title("Real-time Product View Analytics Dashboard")


# ------------------------------------------------------------------ #
# Database Connection
# ------------------------------------------------------------------ #
@st.cache_resource
def get_engine():
    if not sqlalchemy_url:
        st.error("Database connection URL is not configured.")
        st.stop()
    return create_engine(sqlalchemy_url)


engine = get_engine()


def load_data(query, params=None):
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params)
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()


# ------------------------------------------------------------------ #
# Global Filters
# ------------------------------------------------------------------ #
# Get max date from db to use as default instead of CURRENT_DATE
query_max_date = "SELECT MAX(DATE(time_stamp)) as max_date FROM fact_product_views"
df_max_date = load_data(query_max_date)

if not df_max_date.empty and pd.notnull(df_max_date["max_date"].iloc[0]):
    max_date_in_db = df_max_date["max_date"].iloc[0]
else:
    max_date_in_db = pd.Timestamp.now().date()

st.sidebar.header("Dashboard Filters")
selected_date = st.sidebar.date_input("Select Report Date", value=max_date_in_db)
auto_refresh = st.sidebar.checkbox("Auto-refresh (10s)", value=True)

st.markdown(f"### Data Report for: **{selected_date}**")
st.markdown("---")

# ------------------------------------------------------------------ #
# Dashboard Sections
# ------------------------------------------------------------------ #

col1, col2 = st.columns(2)

# 1. Top 10 product_id
with col1:
    st.subheader("1. Top 10 Product IDs by View Count")
    query_top_products = """
                         SELECT p.product_id, COUNT(*) as view_count
                         FROM fact_product_views f
                         JOIN dim_product p ON f.product_key = p.product_key
                         WHERE DATE (f.time_stamp) = :selected_date
                         GROUP BY p.product_id
                         ORDER BY view_count DESC
                             LIMIT 10 \
                         """
    df_top_products = load_data(
        query_top_products, params={"selected_date": selected_date}
    )
    if not df_top_products.empty:
        df_top_products.index = range(1, len(df_top_products) + 1)
        st.dataframe(df_top_products, use_container_width=True)
    else:
        st.info("No data available for this date.")

# 2. Top 10 countries
with col2:
    st.subheader("2. Top 10 Countries by View Count")
    query_top_countries = """
                          SELECT l.country_name, COUNT(*) as view_count
                          FROM fact_product_views f
                                   JOIN dim_location l ON f.location_key = l.location_key
                          WHERE DATE (f.time_stamp) = :selected_date
                          GROUP BY l.country_name
                          ORDER BY view_count DESC
                              LIMIT 10 \
                          """
    df_top_countries = load_data(
        query_top_countries, params={"selected_date": selected_date}
    )
    if not df_top_countries.empty:
        df_top_countries.index = range(1, len(df_top_countries) + 1)
        st.dataframe(df_top_countries, use_container_width=True)
    else:
        st.info("No data available for this date.")

st.markdown("---")

# 3. Top 5 referrer_url
st.subheader("3. Top 5 Referrer URLs by View Count")
query_top_referrers = """
                      SELECT referrer_url, COUNT(*) as view_count
                      FROM fact_product_views
                      WHERE DATE (time_stamp) = :selected_date
                      GROUP BY referrer_url
                      ORDER BY view_count DESC
                          LIMIT 5 \
                      """
df_top_referrers = load_data(
    query_top_referrers, params={"selected_date": selected_date}
)
if not df_top_referrers.empty:
    df_top_referrers.index = range(1, len(df_top_referrers) + 1)
    st.dataframe(df_top_referrers, use_container_width=True)
else:
    st.info("No data available for this date.")

st.markdown("---")

# 4. Store views for a selected country
st.subheader("4. Store Views by Country")
df_all_countries = load_data(
    "SELECT DISTINCT country_name FROM dim_location WHERE country_name IS NOT NULL ORDER BY country_name"
)
country_list = (
    df_all_countries["country_name"].tolist() if not df_all_countries.empty else []
)

if country_list:
    selected_country = st.selectbox("Select a country:", country_list)
    if selected_country:
        query_country_stores = """
                               SELECT s.store_id, COUNT(*) as view_count
                               FROM fact_product_views f
                                        JOIN dim_location l ON f.location_key = l.location_key
                                        JOIN dim_store s ON f.store_key = s.store_key
                               WHERE l.country_name = :country AND DATE (f.time_stamp) = :selected_date
                               GROUP BY s.store_id
                               ORDER BY view_count DESC \
                               """
        df_country_stores = load_data(
            query_country_stores,
            params={"country": selected_country, "selected_date": selected_date},
        )
        if not df_country_stores.empty:
            df_country_stores.index = range(1, len(df_country_stores) + 1)

            col_chart_4, col_data_4 = st.columns([2, 1])
            with col_chart_4:
                # Add a visually appealing Plotly Bar chart
                fig_store = px.bar(
                    df_country_stores,
                    x="store_id",
                    y="view_count",
                    title=f"Store Views in {selected_country}",
                    labels={"store_id": "Store ID", "view_count": "Total Views"},
                    color="view_count",
                    color_continuous_scale="Blues",
                )
                st.plotly_chart(fig_store, use_container_width=True)

            with col_data_4:
                st.markdown("##### Ranking Table")
                st.dataframe(df_country_stores, use_container_width=True)
        else:
            st.info(
                f"No view data found for stores in {selected_country} on this date."
            )
else:
    st.warning("No country data available in the database.")

st.markdown("---")

# 5. Hourly views for a selected product_id
st.subheader("5. Hourly Views for a Product")
df_recent_products = load_data(
    "SELECT DISTINCT p.product_id FROM fact_product_views f JOIN dim_product p ON f.product_key = p.product_key WHERE DATE(f.time_stamp) = :selected_date LIMIT 100",
    params={"selected_date": selected_date},
)
product_suggestions = (
    df_recent_products["product_id"].tolist() if not df_recent_products.empty else []
)

if product_suggestions:
    product_id_input = st.selectbox(
        "Select or type a Product ID:", [""] + product_suggestions
    )
else:
    product_id_input = st.text_input("Enter Product ID:", "")

if product_id_input:
    query_product_hourly = """
                           SELECT EXTRACT(HOUR FROM f.time_stamp) as hour, COUNT(*) as view_count
                           FROM fact_product_views f
                           JOIN dim_product p ON f.product_key = p.product_key
                           WHERE p.product_id = :product_id AND DATE (f.time_stamp) = :selected_date
                           GROUP BY hour
                           ORDER BY hour \
                           """
    df_product_hourly = load_data(
        query_product_hourly,
        params={"product_id": product_id_input, "selected_date": selected_date},
    )
    if not df_product_hourly.empty:
        # Display Total Views as a prominent KPI metric
        total_views = df_product_hourly["view_count"].sum()
        st.metric(
            label=f"Total Views for **{product_id_input}**", value=int(total_views)
        )

        col_chart_5, col_data_5 = st.columns([2, 1])
        with col_chart_5:
            # Use Plotly Area chart for a sleek hourly time-series view
            fig_hourly = px.area(
                df_product_hourly,
                x="hour",
                y="view_count",
                title="Hourly View Trend",
                markers=True,
                labels={"hour": "Hour of Day", "view_count": "Views"},
                color_discrete_sequence=["#ff7f0e"],
            )
            # Make sure X-axis displays hours nicely
            fig_hourly.update_xaxes(dtick=1)
            st.plotly_chart(fig_hourly, use_container_width=True)

        with col_data_5:
            st.markdown("##### Hourly Data")
            st.dataframe(df_product_hourly.set_index("hour"), use_container_width=True)
    else:
        st.info("No view data for this product on this date.")

st.markdown("---")

# 6. Views by Browser and OS
st.subheader("6. Views Distribution by Browser and OS")
query_browser_os = """
                   SELECT d.browser,
                          d.os,
                          COUNT(*) as view_count
                   FROM fact_product_views f
                            JOIN dim_device d ON f.device_key = d.device_key
                   WHERE DATE (f.time_stamp) = :selected_date
                   GROUP BY d.browser, d.os \
                   """
df_browser_os = load_data(query_browser_os, params={"selected_date": selected_date})

if not df_browser_os.empty:
    # Summarize data for pie charts
    df_browser = df_browser_os.groupby("browser")["view_count"].sum().reset_index()
    df_os = df_browser_os.groupby("os")["view_count"].sum().reset_index()

    col_pie1, col_pie2 = st.columns(2)

    with col_pie1:
        fig_browser = px.pie(
            df_browser,
            values="view_count",
            names="browser",
            title="Browser Distribution",
            hole=0.3,
        )
        st.plotly_chart(fig_browser, use_container_width=True)

    with col_pie2:
        fig_os = px.pie(
            df_os, values="view_count", names="os", title="OS Distribution", hole=0.3
        )
        st.plotly_chart(fig_os, use_container_width=True)

    # Also show the detailed dataframe if they still want to see it
    st.markdown("##### Detailed Breakdown")
    df_browser_os = df_browser_os.sort_values(by="view_count", ascending=False)
    df_browser_os.index = range(1, len(df_browser_os) + 1)
    st.dataframe(df_browser_os, use_container_width=True)
else:
    st.info("No data available for browser/os on this date.")

# ------------------------------------------------------------------ #
# Auto-refresh Logic
# ------------------------------------------------------------------ #
if auto_refresh:
    time.sleep(10)
    st.rerun()
