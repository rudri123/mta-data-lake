import streamlit as st
import pandas as pd
import plotly.express as px
import os
import glob

st.set_page_config(page_title="MTA Data Lake Dashboard", page_icon="🚆", layout="wide")

st.title("🚆 MTA Subway Ridership Dashboard")
st.markdown("This dashboard visualizes the processed transit data stored in your local data lake.")

@st.cache_data
def load_latest_data():
    """Find and load the most recent processed CSV file from the data/ directory."""
    processed_files = glob.glob(os.path.join("data", "*", "mta_ridership_processed.csv"))
    if not processed_files:
        return None
    # Sort by creation time / name since they have timestamped strings
    latest_file = max(processed_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)
    return df, latest_file

data_result = load_latest_data()

if data_result is None:
    st.warning("No processed data found. Please run `python src/pipeline.py --local` first.")
    st.stop()

df, file_path = data_result
st.success(f"Data successfully loaded from `{file_path}`")

# ---- KPIs ----
st.subheader("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

total_ridership = df['ridership'].sum()
total_stations = df['station_complex'].nunique()
total_boroughs = df['borough'].nunique()
latest_date = df['date'].max()

col1.metric("Total Ridership", f"{total_ridership:,}")
col2.metric("Unique Station Complexes", total_stations)
col3.metric("Boroughs Tracked", total_boroughs)
col4.metric("Latest Date in Data", latest_date)

st.markdown("---")

# ---- Visualizations ----
col_viz1, col_viz2 = st.columns(2)

with col_viz1:
    st.subheader("Ridership by Borough")
    borough_df = df.groupby("borough")['ridership'].sum().reset_index()
    fig_borough = px.pie(borough_df, names='borough', values='ridership', hole=0.4, 
                         color_discrete_sequence=px.colors.sequential.RdBu)
    st.plotly_chart(fig_borough, use_container_width=True)

with col_viz2:
    st.subheader("Top 10 Busiest Stations")
    station_df = df.groupby("station_complex")['ridership'].sum().reset_index()
    station_df = station_df.sort_values(by="ridership", ascending=False).head(10)
    fig_station = px.bar(station_df, x="ridership", y="station_complex", orientation='h',
                         color='ridership', color_continuous_scale='Viridis')
    fig_station.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_station, use_container_width=True)

st.markdown("---")

# ---- Map ----
st.subheader("Station Ridership Heatmap")
st.markdown("A geographic breakdown of the busiest stations. Larger and brighter circles indicate higher transit volume.")

# Aggregate total ridership per station for proper map scaling
map_agg_df = df.groupby(["station_complex", "latitude", "longitude"])["ridership"].sum().reset_index()

fig_map = px.scatter_mapbox(
    map_agg_df, 
    lat="latitude", 
    lon="longitude", 
    size="ridership", 
    color="ridership",
    hover_name="station_complex", 
    color_continuous_scale=px.colors.sequential.Plasma,
    size_max=30, 
    zoom=10,
    mapbox_style="carto-positron"
)
fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

st.plotly_chart(fig_map, use_container_width=True)

st.markdown("---")

# ---- Raw Data Expander ----
with st.expander("Explore Processed Data"):
    st.dataframe(df.head(1000))
