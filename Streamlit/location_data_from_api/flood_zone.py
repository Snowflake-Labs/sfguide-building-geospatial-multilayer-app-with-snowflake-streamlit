# Import python packages
import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()

#st.set_page_config(layout='wide')

logo = 'snowflake_logo_color_rgb.svg'
with open('extra.css') as ab:
    st.markdown(f"<style>{ab.read()}</style>", unsafe_allow_html=True)

    
st.logo(logo)


def polygon(data):
    # create a new data frame filter the dataframe where the type in each geography field contains the word 'Polygon'
    dataP = data.filter(call_function('ST_ASGEOJSON',col('GEOGRAPHY'))['type'].astype(StringType())=='Polygon')
    # create a new dataframe and Filter the dataframe where the type in each geography field contains the word 'Multi Polygon'
    dataM = data.filter(call_function('ST_ASGEOJSON',col('GEOGRAPHY'))['type'].astype(StringType())=='MultiPolygon')

    ## use the join table function to flatten the multi polygon into one row per polygon
    dataM = dataM.join_table_function('flatten',
                                        call_function('ST_ASGEOJSON',
                                        col('GEOGRAPHY'))['coordinates']).drop('SEQ',
                                                                               'KEY',
                                                                               'PATH',
                                                                               'INDEX',
                                                                               'THIS')                                                                                                        
    
    ## With the flattend results, create a new valid geography object with the type 'Polygon'
    dataM = dataM.with_column('GEOGRAPHY',
                                to_geography(object_construct(lit('coordinates'),
                                                        to_array('VALUE'),
                                                        lit('type'),
                                                        lit('Polygon')))).drop('VALUE')

    ### return both the converted polygons (dataM) as well as the already single polygons (dataP) into one dataframe

    return dataM.union(dataP).with_column_renamed('GEOGRAPHY','POLYGON')



st.markdown(
    """
    <style>
    .heading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */
        
        padding: 10px;  /* add padding around the content */
    }
    .tabheading{
        background-color: rgb(41, 181, 232);  /* light blue background */
        color: white;  /* white text */;
        font-size: 24px;
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


st.markdown('<h0black>URBAN AREAS IN </h0black> <h0blue>FLOOD ZONES</h0blue><BR>', unsafe_allow_html=True)

st.markdown('<h1blue>CURRENT WARNINGS</h1blue><BR>', unsafe_allow_html=True)

#------current flood warnings


tooltip = {
   "html": """ 
   <br> <b>River or Sea: </b> {River or Sea} 
   <br> <b>Description: </b> {DESCRIPTION}
   <br> <b>Area Name: </b> {is Tidal}
   <br> <b>Message: </b> {Message}
   <br> <b>Severity Level:</b> {Severity Level}
   <br> <b>Time Raised:</b> {Time Raised}
   
   """,
   "style": {
       "width":"50%",
        "backgroundColor": "steelblue",
        "color": "white",
       "text-wrap": "balance"
   }
}



import pydeck as pdk
import json


data = session.table('DEFAULT_SCHEMA.LATEST_FLOOD_WARNINGS')
data = data.filter(col('VALID')==1)
data = data.with_column('GEOGRAPHY',to_geography('GEOGRAPHY'))


data = polygon(data)



center = data.select('LAT','LON')


LAT = center.select('LAT').to_pandas().LAT.iloc[0]
LON = center.select('LON').to_pandas().LON.iloc[0]



datapd1 = data.to_pandas()
datapd1["POLYGON"] = datapd1["POLYGON"].apply(lambda row: json.loads(row)["coordinates"])




# Create data layer for each polygon
data_layer = pdk.Layer(
    "PolygonLayer",
    datapd1,
    opacity=0.3,
    get_polygon="POLYGON", 
    filled=True,
    get_fill_color=[255,159,54],
    get_line_color=[0, 0, 0],
    auto_highlight=True,
    pickable=True,
)

# Set the view on the map
view_state = pdk.ViewState(
    longitude=LON,
    latitude=LAT,
    zoom=7,  # Adjust zoom if needed
    pitch=0,
)



# Render the map with layer and tooltip
r = pdk.Deck(
    layers=[data_layer],
    initial_view_state=view_state,
    map_style=None,
    tooltip=tooltip)
    
st.pydeck_chart(r, use_container_width=True)






#-----








tooltip = {
   "html": """ 
   <br> <b>UPRN:</b> {UPRN},
   <br> <b>Number of UPRNs:</b> {NUMBER_UPRN} 
   <br> <b>Description:</b> {DESCRIPTION}
   <br> <b>Roof Material:</b> {ROOFMATERIAL_PRIMARYMATERIAL}
   <br> <b>Solar Panel Presence:</b> {ROOFMATERIAL_SOLARPANELPRESENCE}
   <br> <b>Roof Shape:</b> {ROOFSHAPEASPECT_SHAPE}
   <br> <b>Geometry Area M2:</b> {GEOMETRY_AREA_M2}
   
   """,
   "style": {
       "width":"50%",
        "backgroundColor": "steelblue",
        "color": "white",
       "text-wrap": "balance"
   }
}


# Populate dataframe from query

data = session.table('DEFAULT_SCHEMA.BUILDINGS_IN_FLOOD_AREAS')

LABELS = data.select(col('"label"')).distinct().to_pandas()
selected_label = st.selectbox('Select Area:',LABELS)

data = data.filter(col('"label"')==selected_label)
#st.write(data.limit(1))
LAT = data.select('LAT').to_pandas().LAT.iloc[0]
LON = data.select('LON').to_pandas().LON.iloc[0]


datapd = data.group_by(
                     
                     'DESCRIPTION',
                     'ROOFMATERIAL_PRIMARYMATERIAL',
                     'ROOFMATERIAL_SOLARPANELPRESENCE',
                    'ROOFSHAPEASPECT_SHAPE',
                    'GEOMETRY_AREA_M2').agg(array_to_string(array_agg('UPRN'),lit(', ')).alias('UPRN'),approx_count_distinct('UPRN').alias('NUMBER_UPRN'),any_value('GEOGRAPHY').alias('GEOGRAPHY')).to_pandas()
st.write(datapd.head(2))
datapd["GEOGRAPHY"] = datapd["GEOGRAPHY"].apply(lambda row: json.loads(row)["coordinates"])

st.markdown('<h1blue>BUILDINGS IN SELECTED ZONE</h1blue><BR>', unsafe_allow_html=True)

# Create data layer - this where the geometry is likely failing - column is now called geometry to match geopandas default
building_layer = pdk.Layer(
    "PolygonLayer",
    datapd,
    opacity=0.3,
    elevation_range=[1, 10],
    extruded=False,
    height='NUMBER_UPRN',
    get_polygon="GEOGRAPHY", 
    filled=True,
    pitch=90.5,
    get_fill_color=[41, 181, 232],
    get_line_color=[0, 0, 0],
    get_line_width=0.1,
    auto_highlight=True,
    bearing=-27.36,
    pickable=True,
)

data_layer = pdk.Layer(
    "PolygonLayer",
    datapd1,
    opacity=0.3,
    get_polygon="POLYGON", 
    filled=True,
    get_fill_color=[255,159,54],
    get_line_color=[0, 0, 0],
    auto_highlight=True,
    pickable=True,
)

# Set the view on the map
view_state = pdk.ViewState(
    longitude=LON,
    latitude=LAT,
    zoom=13,  # Adjust zoom if needed
    pitch=0,
)



# Render the map with layer and tooltip
r = pdk.Deck(
    layers=[data_layer, building_layer],
    initial_view_state=view_state,
    map_style=None,
    tooltip=tooltip)
    
st.pydeck_chart(r, use_container_width=True)