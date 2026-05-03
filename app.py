#%%
import json

import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np
import folium
import requests
import plotly.express as px
from streamlit_folium import st_folium

#%%
# Load data
client = bigquery.Client()


# ---------------------------------------------------------
# CACHED DATA FETCHER
# @st.cache_data ensures we only run the query once,
# preventing unnecessary BigQuery costs on UI refreshes.
# ---------------------------------------------------------
@st.cache_data(ttl=3600)  # Cache clears every hour
# def load_data(query_string):
#     try:
#         df = client.query(query_string).to_dataframe()
#         return df
#     except Exception as e:
#         st.error(f"Error fetching data: {e}")
#         return pd.DataFrame()

def load_data():
    try:
        # Read the highly compressed Parquet file directly from Cloud Storage
        gcs_path = "gs://housing-app-494519-dashboard-data/sales_listing.parquet"
        df = pd.read_parquet(gcs_path)
        return df
    except Exception as e:
        st.error(f"Error fetching data from GCS: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=86400)
def load_la_county_zip_geojson():
    """
    LA County ZIP polygons (official GIS). Paginates ArcGIS query so JSON is valid.
    The old GitHub URL (blackmad/.../la-county-zip-codes.geojson) 404s; HTML responses
    cause JSONDecodeError / 'Extra data' when Folium parses them as GeoJSON.
    """
    base = (
        "https://public.gis.lacounty.gov/public/rest/services/"
        "LACounty_Dynamic/Administrative_Boundaries/MapServer/5/query"
    )
    params_base = {
        "where": "1=1",
        "outFields": "ZIPCODE",
        "returnGeometry": "true",
        "f": "geojson",
        "outSR": "4326",
        "maxAllowableOffset": "0.0005"
    }
    features = []
    offset = 0
    while True:
        r = requests.get(
            base,
            params={**params_base, "resultOffset": offset},
            timeout=180,
        )
        r.raise_for_status()
        try:
            chunk = r.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"ZIP boundary API returned non-JSON (HTTP {r.status_code}). "
                f"First bytes: {r.text[:120]!r}"
            ) from e
        batch = chunk.get("features", [])
        features.extend(batch)
        if not chunk.get("exceededTransferLimit"):
            break
        offset += len(batch)
        if not batch:
            break
    return {"type": "FeatureCollection", "features": features}


BQ_QUERY = """
    SELECT *
    FROM `housing-app-494519.la_housing.sales_listing`
"""

# ---------------------------------------------------------
# UI — set_page_config must be the first Streamlit call
# ---------------------------------------------------------
st.set_page_config(page_title="BigQuery Data Explorer", layout="wide")
st.markdown(
    """
    <style>
    .main h1 { margin-bottom: 0.25rem !important; }
    div[data-testid="stDivider"] { margin-top: 0.25rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Los Angeles Housing Dashboard")
st.divider()

#data = load_data(BQ_QUERY)
data = load_data()
data["removedDate"] = pd.to_datetime(data["removedDate"], utc=True)
#%%
# KPIs
active_df = data[data.status == "Active"]
active_listing = int(len(active_df))
avg_price = (
    float(np.median(active_df["price"])) if len(active_df) else float("nan")
)
avg_days_on_market = (
    float(np.median(active_df["daysOnMarket"])) if len(active_df) else float("nan")
)
sold_l7d = int(
    len(
        data[
            data["removedDate"]
            >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
        ]
    )
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Active listings", f"{active_listing:,.0f}")
c2.metric(
    "Avg Price",
    f"${int(round(avg_price)):,.0f}" if pd.notna(avg_price) else "—",
)
c3.metric(
    "Avg Days on Market",
    f"{avg_days_on_market:.0f}" if pd.notna(avg_days_on_market) else "—",
)
c4.metric("Sold in Last 7 Days", f"{sold_l7d:,.0f}")
st.divider()
# %%
# Choropleth: average active listing price by ZIP (LA County boundaries)
active_map = active_df.dropna(subset=["zipCode", "price"]).copy()
active_map["zipCode"] = (
    active_map["zipCode"]
    .astype(str)
    .str.replace(r"\.0$", "", regex=True)
    .str.zfill(5)
)
heat_price = active_map.groupby("zipCode", as_index=False)["price"].mean()
heat_price['price'] = heat_price['price']/1000
#%%
plot1,plot2 = st.columns([2,1],gap="small")
with plot1:
    if not heat_price.empty:
        try:
            la_geo = load_la_county_zip_geojson()
        except Exception as e:
            st.error(f"Could not load LA County ZIP boundaries: {e}")
        else:
            m = folium.Map(
                location=[34.0522, -118.2437], zoom_start=9.5, tiles="cartodbpositron"
            )
            folium.Choropleth(
                geo_data=la_geo,
                name="choropleth",
                data=heat_price,
                columns=["zipCode", "price"],
                key_on="feature.properties.ZIPCODE",
                fill_color="Reds",
                fill_opacity=0.7,
                line_opacity=0.2,
                legend_name="Average house price in K ($)",
                nan_fill_color="white",
            ).add_to(m)

            st_folium(m, use_container_width=True, height=300,returned_objects=[])
    else:
        st.warning("No active listings with ZIP and price to map.")
with plot2:
    price_col = active_df[active_df['price']<2000000]
    if not price_col.empty:
        # 2. Create Histogram using Plotly
        # x: price, nbins: 100
        fig = px.histogram(
            price_col, 
            x="price", 
            nbins=60,
            title="Price Distribution (100 Bins)",
            labels={'price': 'Price ($)', 'count': 'Number of Listings'},
            color_discrete_sequence=['#cb181d'] # Matches your red heatmap theme
        )

        # Improve layout
        fig.update_layout(
            xaxis_title="Price ($)",
            yaxis_title=None,
            bargap=0.1,
            height = 350,
            width = 600,
            margin=dict(l=0, r=0, t=0, b=0)
        )

        # 3. Display in Streamlit
        #st.plotly_chart(fig, use_container_width=True)
        #fig.update_layout(width=600,height=300) 
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No price data available to generate histogram.")
# %%
#price histogram

# %%
