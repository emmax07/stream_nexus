import os
import pandas as pd
import streamlit as st
import plotly.express as px
import snowflake.connector
from dotenv import load_dotenv


# Load Environment Variables
load_dotenv()


# Streamlit Page Config
st.set_page_config(
    page_title="Ride Streaming Dashboard",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 Real-Time Ride Streaming Dashboard")
st.markdown("Kafka → Snowflake Data Pipeline")

# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

conn = get_connection()


# Load Data
query = """
SELECT *
FROM rides
ORDER BY timestamp DESC
LIMIT 10000
"""

df = pd.read_sql(query, conn)

if df.empty:
    st.warning("No data available.")
    st.stop()


# KPI Section
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Rides",
        len(df)
    )

with col2:
    st.metric(
        "Average Fare",
        f"${df['FARE'].mean():.2f}"
    )

with col3:
    st.metric(
        "Unique Locations",
        df["LOCATION"].nunique()
    )


# Top Cities
st.subheader("Top Ride Locations")

location_df = (
    df.groupby("LOCATION")
      .size()
      .reset_index(name="TOTAL_RIDES")
      .sort_values("TOTAL_RIDES", ascending=False)
)

fig = px.bar(
    location_df,
    x="LOCATION",
    y="TOTAL_RIDES",
    title="Ride Volume by City"
)

st.plotly_chart(fig, use_container_width=True)


# Area Analysis
st.subheader("Most Popular Areas")

area_df = (
    df.groupby("AREA")
      .size()
      .reset_index(name="TOTAL_RIDES")
      .sort_values("TOTAL_RIDES", ascending=False)
      .head(10)
)

fig2 = px.bar(
    area_df,
    x="AREA",
    y="TOTAL_RIDES",
    title="Top 10 Areas"
)

st.plotly_chart(fig2, use_container_width=True)


# Fare Distribution
st.subheader("Fare Distribution")

fig3 = px.histogram(
    df,
    x="FARE",
    nbins=20,
    title="Ride Fare Distribution"
)

st.plotly_chart(fig3, use_container_width=True)

# Recent Ride Activity
st.subheader("Latest Ride Records")

st.dataframe(
    df.head(25),
    use_container_width=True
)