import json
import streamlit as st
import pandas as pd
import pydeck as pdk
import json
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
from snowflake.snowpark.context import get_active_session
session = get_active_session()
st.set_page_config(layout="wide")
logo = 'snowflake_logo_color_rgb.svg'
with open('extra.css') as ab:
    st.markdown(f"<style>{ab.read()}</style>", unsafe_allow_html=True)

    
st.logo(logo)
st.markdown(
    """
    <style>
    .heading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        padding: 30px;  /* add padding around the content */
    }
    .tabheading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        padding: 10px;  /* add padding around the content */
    }
    .veh1 {
        color: rgb(125, 68, 207);  /* purple */
    }
    .veh2 {
        color: rgb(212, 91, 144);  /* pink */
    }
    .veh3 {
        color: rgb(255, 159, 54);  /* orange */
    }
    .veh4 {
        padding: 10px;  /* add padding around the content */
        color: rgb(0,53,69);  /* midnight */
    }
    .veh5 {
        padding: 10px;  /* add padding around the content */
        color: rgb(138,153,158);  /* windy city */
        font-size: 14px
    }
    
    body {
        color: rgb(0,53,69);
    }
    
    div[role="tablist"] > div[aria-selected="true"] {
        background-color: rgb(41, 181, 232);
        color: rgb(0,53,69);  /* Change the text color if needed */
    }

    
    </style>
    """,
    unsafe_allow_html=True
)


st.markdown('<h0black>BRITISH TOWNS FOR </h0black><h0blue>URBAN PLANNING</h0blue><BR>', unsafe_allow_html=True)

tooltip = {
   "html": """<h1grey>Name:</h1grey> <h1sub>{NAME_1} <br> <h1grey>Form of Way:</h1grey> {FORM_OF_WAY} <br> <h1grey>Length:</h1grey> {LENGTH},<br> <h1grey>Unique Property Reference Number:</h1grey> {UPRN}""",
   "style": {
       "width":"50%",
        "backgroundColor": "white",
        "color": "white",
       "text-wrap": "balance"
   }
}




road_links = session.table('DEFAULT_SCHEMA.ROAD_LINKS')
road_links = road_links.with_column('UPRN',lit('N/A'))
urban_extents = session.table('DEFAULT_SCHEMA.URBAN_EXTENTS').with_column_renamed('NAME1_TEXT','NAME_1')

road_nodes = session.table('DEFAULT_SCHEMA.ROAD_NODES')
road_nodes = road_nodes.with_column('UPRN',lit('N/A'))
BUILDINGS = session.table('DEFAULT_SCHEMA.BUILDINGS_WITH_UPRN')



filter = BUILDINGS.select('NAME1_TEXT').distinct()
filter = st.selectbox('Choose Town:',filter,7)
data = urban_extents.filter(col('NAME_1')==filter)


datajoined = data.drop('NAME_1').join(road_links,call_function('ST_INTERSECTS',data['POLYGON'],road_links['LINESTRING']))



nodes_filtered = road_nodes.join(datajoined.select('LINESTRING'),call_function('ST_INTERSECTS',
                                         datajoined['LINESTRING'],road_nodes['POINT'])).drop('LINESTRING')

BUILDINGS = BUILDINGS.filter(col('NAME1_TEXT')==filter).select('NAME1_TEXT','GEOMETRY','CLASS','SUBTYPE','UPRN')

BUILDINGS = BUILDINGS.with_column_renamed('NAME1_TEXT','NAME_1')
BUILDINGS = BUILDINGS.with_column('FORM_OF_WAY',lit('N/A'))
BUILDINGS = BUILDINGS.with_column('LENGTH',lit('N/A'))
data3pd = BUILDINGS.limit(10000).to_pandas()




data = data.with_column('FORM_OF_WAY',lit('N/A'))
data = data.with_column('LENGTH',lit('N/A'))

town_metrics = data.limit(1).to_pandas()



col1,col2,col3,col4,col5 = st.columns(5)
with col1:
    st.metric(label='Area Hectares:', value=town_metrics.AREAHECTARES.iloc[0])
with col2:
    st.metric('Geometry Area M:', town_metrics.GEOMETRY_AREA_M.iloc[0])
with col3:
    st.metric('Number of Road Links:',datajoined.count())
with col4:
    st.metric('Number of Nodes:',nodes_filtered.count())
with col5:
    st.metric('Number of Buildings:',BUILDINGS.count())

centre = data.with_column('CENTROID',call_function('ST_CENTROID',col('POLYGON')))
centre = centre.with_column('LON',call_function('ST_X',col('CENTROID')))
centre = centre.with_column('LAT',call_function('ST_Y',col('CENTROID')))

datajoined = datajoined.drop('POLYGON')

centrepd = centre.select('LON','LAT').to_pandas()
LON = centrepd.LON.iloc[0]
LAT = centrepd.LAT.iloc[0]
# Populate dataframe from query

datapd = datajoined.to_pandas()
data2pd = data.to_pandas()
datapd["LINESTRING"] = datapd["LINESTRING"].apply(lambda row: json.loads(row)["coordinates"])
data2pd["POLYGON"] = data2pd["POLYGON"].apply(lambda row: json.loads(row)["coordinates"])
data3pd["GEOMETRY"] = data3pd["GEOMETRY"].apply(lambda row: json.loads(row)["coordinates"])




# Create data layer - this where the geometry is likely failing - column is now called geometry to match geopandas default

building_layer = pdk.Layer(
    "PolygonLayer",
    data3pd,
    opacity=1,
    get_polygon="GEOMETRY", 
    filled=True,
    get_fill_color=[252, 181, 108],
    get_line_color=[0, 0, 0],
    auto_highlight=True,
    pickable=True,
)




network_layer  = pdk.Layer(
        type="PathLayer",
        data=datapd,
        pickable=True,
        get_color=[41, 181, 232],
        width_scale=5,
        opacity = 1,
        width_min_pixels=2,
        get_path="LINESTRING",
        get_width=2,
)

extent_layer = pdk.Layer(
    "PolygonLayer",
    data2pd,
    opacity=0.3,
    get_polygon="POLYGON", 
    filled=True,
    get_fill_color=[198, 228, 241],
    get_line_color=[0, 0, 0],
    auto_highlight=True,
    pickable=False,
)

datajoined = datajoined.drop('POLYGON')

nodes_filtered_2 = nodes_filtered.with_column('LAT',call_function('ST_Y',col('POINT')))
nodes_filtered_2 = nodes_filtered_2.with_column('LON',call_function('ST_X',col('POINT')))
nodes_filtered_2 = nodes_filtered_2.drop('POINT')
# Create data layer - this where the geometry is likely failing - column is now called geometry to match geopandas default
nodes = pdk.Layer(
            'ScatterplotLayer',
            data=nodes_filtered_2.to_pandas(),
            get_position='[LON, LAT]',
            get_color=[17,86,127],
            get_radius=15,
            pickable=True)

# Set the view on the map
view_state = pdk.ViewState(
    longitude=LON,
    latitude=LAT,
    zoom=13,  # Adjust zoom if needed
    pitch=0,
)



# Render the map with layer and tooltip
r = pdk.Deck(
    layers=[extent_layer,network_layer,nodes,building_layer],
    initial_view_state=view_state,
    map_style=None,
    tooltip=tooltip)
    
st.pydeck_chart(r, use_container_width=True, height = 900)