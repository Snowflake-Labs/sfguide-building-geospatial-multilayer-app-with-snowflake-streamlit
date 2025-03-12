import json
import streamlit as st
import pandas as pd
import pydeck as pdk
import json
import datetime
from snowflake.snowpark.context import get_active_session
session = get_active_session()
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
st.set_page_config(layout="wide")



    

def selected_postcode(postcode):
    return session.table('DEFAULT_SCHEMA.POSTCODES').filter(col('NAME1')==postcode).select('GEOGRAPHY','PC_SECT')

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

image = 'https://www.snowflake.com/wp-content/themes/snowflake/assets/img/brand-guidelines/logo-white-example.svg'



with st.sidebar:
    st.image(image)
    st.caption('Modify the estimated energy % irradiation for each slope direction')
    south_facing = st.slider('South Facing:',0.0,1.0,1.0)
    south_east_facing = st.slider('South East Facing:',0.0,1.0,0.90)
    south_west_facing = st.slider('South West Facing:',0.0,1.0,0.80)
    west_facing = st.slider('West Facing:',0.0,1.0,0.70)
    north_east_facing = st.slider('North East Facing:',0.0,1.0,0.60)
    north_west_facing = st.slider('North West Facing:',0.0,1.0,0.60)
    east_facing = st.slider('East Facing:',0.0,1.0,0.30)
    north_facing = st.slider('North Facing:',0.0,1.0,0.10)

    solar_panel_angle = st.slider('Solar Panel Elevation Angle:',0,90,30)

    solar_joules_per_s = 1000
    panel_rating_W = 300
    size = 1.89

st.markdown('<h1 class="heading">SOLAR POWER ENERGY INSIGHTS</h2><BR>', unsafe_allow_html=True)


tooltip = {
   "html": """ 
   <br> <b>Theme:</b> {THEME} 
   <br> <b>Description:</b> {DESCRIPTION}
   <br> <b>Roof Material:</b> {ROOFMATERIAL_PRIMARYMATERIAL}
   <br> <b>Solar Panel Presence:</b> {ROOFMATERIAL_SOLARPANELPRESENCE}
   <br> <b>Green Proof Presence:</b> {ROOFMATERIAL_GREENROOFPRESENCE}
   <br> <b>Roof Shape:</b> {ROOFSHAPEASPECT_SHAPE}
   <br> <b>Geometry Area M2:</b> {GE}
   <br> <b>Area Pitched M2:</b> {A}
   <br> <b>Area Flat M2:</b> {RF}
   <br> <b>Direct Irradiance M2:</b> {D}
   <br> <b>Efficiency Ratio:</b> {EFFICIENCY_RATIO}
   """,
   "style": {
       "width":"50%",
        "backgroundColor": "steelblue",
        "color": "white",
       "text-wrap": "balance"
   }
}
BUILDINGS = session.table('DEFAULT_SCHEMA.BUILDINGS_WITH_ROOF_SPECS')
SOLAR_ELEVATION_DF = session.table('DEFAULT_SCHEMA.SOLAR_ELEVATION')

st.markdown('<h3 class="veh3">SEARCH FOR BUILDINGS</h3><BR>',unsafe_allow_html=True)

col1,col2,col3, col4 = st.columns(4)


with col1:
    filter = BUILDINGS.select('NAME1_TEXT').distinct()
    filter = st.selectbox('Choose Town:',filter, 9)
with col2:
    postcodes = session.table('DEFAULT_SCHEMA.POSTCODES').filter(col('NAME1_TEXT')==filter)
    postcodef = st.selectbox('Postcode:',postcodes, 10)
with col3:
    distance = st.number_input('Distance in M:', 20,2000,500)
with col4:
    selected_date = st.date_input('Date for Solar Elevation Angle:',datetime.date(2024,1, 1),datetime.date(2024,1,1),datetime.date(2024,12,31))
st.divider()
SOLAR_ELEVATION_DF_FILTERED = SOLAR_ELEVATION_DF.filter(call_function('date',col('"Validity_date_and_time"'))==selected_date)

BUILDINGS = BUILDINGS.filter(col('NAME1_TEXT')==filter)
selected_point = selected_postcode(postcodef)
BUILDINGS = BUILDINGS.join(selected_point.with_column_renamed('GEOGRAPHY','SPOINT'),
                           call_function('ST_DWITHIN',selected_point['GEOGRAPHY'],
                                        BUILDINGS['GEOGRAPHY'],distance)).drop('SPOINT')


BUILDINGS = BUILDINGS.with_column('DIRECT_IRRADIANCE_M2',
                                  col('ROOFSHAPEASPECT_AREAFLAT_M2')+
                                  col('ROOFSHAPEASPECT_AREAFACINGNORTH_M2')*north_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGSOUTH_M2')*south_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGEAST_M2')*east_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGWEST_M2')*west_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGNORTHWEST_M2')*north_east_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGNORTHEAST_M2')*north_west_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGSOUTHEAST_M2')*south_east_facing +
                                 col('ROOFSHAPEASPECT_AREAFACINGSOUTHWEST_M2')*south_west_facing)



SOLAR_BUILDINGS_SUM = BUILDINGS.agg(sum('DIRECT_IRRADIANCE_M2').alias('DIRECT_IRRADIANCE_M2'),
                                   sum('GEOMETRY_AREA_M2').alias('TOTAL_AREA')).join(SOLAR_ELEVATION_DF_FILTERED.group_by('"Validity_date_and_time"').agg(avg('"Solar_elevation_angle"').alias('"Solar_elevation_angle"')))
SOLAR_BUILDINGS_SUM = SOLAR_BUILDINGS_SUM.with_column('total_energy',when(col('"Solar_elevation_angle"')<0,0).otherwise(col('DIRECT_IRRADIANCE_M2')*cos(radians(lit(solar_panel_angle))-col('"Solar_elevation_angle"'))))
                                                                                                     
st.markdown('<h4 class="veh5"> TOTAL AREA AVAILABLE FOR ENERGY CONVERSION</h4>',unsafe_allow_html=True)
with st.expander('View Time Analysis'):
    st.bar_chart(SOLAR_BUILDINGS_SUM.to_pandas(),y='TOTAL_ENERGY',x='Validity_date_and_time', color='#29B5E8')

st.divider()





BUILDINGS_V = BUILDINGS.limit(2000).select('GEOGRAPHY',
                                                               'THEME',
                                                               'DESCRIPTION',
                                                               col('GEOMETRY_AREA_M2').astype(StringType()).alias('GE'),
                                                            'ROOFMATERIAL_PRIMARYMATERIAL',
                                                             'ROOFMATERIAL_SOLARPANELPRESENCE',
                                                             'ROOFMATERIAL_GREENROOFPRESENCE',
                                                              'ROOFSHAPEASPECT_SHAPE',
                                                              col('ROOFSHAPEASPECT_AREAPITCHED_M2').astype(StringType()).alias('A'),
                                                              col('ROOFSHAPEASPECT_AREAFLAT_M2').astype(StringType()).alias('RF'),
                                                              col('DIRECT_IRRADIANCE_M2').alias('D'),
                                                                div0(col('D'),col('GEOMETRY_AREA_M2')).alias('EFFICIENCY_RATIO'),
                                                                when(col('EFFICIENCY_RATIO')>=0.9,[255,159,54]).when(col('EFFICIENCY_RATIO')>=0.8,[212, 91, 144]).otherwise([41,181,232]).alias('COLOR'),
                                                                    col('COLOR')[0].alias('R'),
                                                                    col('COLOR')[1].alias('G'),
                                                                    col('COLOR')[2].alias('B'))
                                          
                                                              




centre = selected_point
centre = centre.with_column('LON',call_function('ST_X',col('GEOGRAPHY')))
centre = centre.with_column('LAT',call_function('ST_Y',col('GEOGRAPHY')))




centrepd = centre.select('LON','LAT').to_pandas()
LON = centrepd.LON.iloc[0]
LAT = centrepd.LAT.iloc[0]
# Populate dataframe from query

datapd = BUILDINGS_V.to_pandas()

datapd["coordinates"] = datapd["GEOGRAPHY"].apply(lambda row: json.loads(row)["coordinates"])

st.markdown('<h3 class="veh3">BUILDINGS COLOR CODED BY SOLAR POTENTIAL EFFICIENCY',unsafe_allow_html=True)

# Create data layer - this where the geometry is likely failing - column is now called geometry to match geopandas default
data_layer = pdk.Layer(
    "PolygonLayer",
    datapd,
    opacity=1,
    get_polygon="coordinates", 
    filled=True,
    get_fill_color=["R-1","G-1","B-1"],
    get_line_color=[0, 0, 0],
    get_line_width=0.3,
    auto_highlight=True,
    pickable=True,
)

# Set the view on the map
view_state = pdk.ViewState(
    longitude=LON,
    latitude=LAT,
    zoom=15,  # Adjust zoom if needed
    pitch=0,
)



# Render the map with layer and tooltip
r = pdk.Deck(
    layers=[data_layer],
    initial_view_state=view_state,
    map_style=None,
    tooltip=tooltip)
    
st.pydeck_chart(r, use_container_width=True,height=700)

st.caption('Colour key for the buildings.  Efficiency Ratio is the total area divided by the solar coverage lost by the pitch of each roof')

st.markdown('''|<span style="color: rgb(0,53,69); font-size: 24px; font-weight: 400;"> EFFICIENCY RATIO </span>   | <span style="color: rgb(0,53,69); font-size: 24px; font-weight: 400;">COLOUR</span> |
|------------------------------|--------------|
| <span style="color: rgb(255,159,54); font-size: 40px;">≥ 0.9</span>                        | <span style="color: rgb(255,159,54); font-size: 40px;">■</span> |
| <span style="color: rgb(212,91,144); font-size: 40px;">≥ 0.8 and < 0.9 </span>             | <span style="color: rgb(212,91,144); font-size: 40px;">■</span> |
| <span style="color: rgb(41,181,232); font-size: 40px;">< 0.8  </span>                      | <span style="color: rgb(41,181,232); font-size: 40px;">■</span> |''', unsafe_allow_html=True)



