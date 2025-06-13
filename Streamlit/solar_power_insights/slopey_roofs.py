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

### ADD THEMING
logo = 'snowflake_logo_color_rgb.svg'
with open('extra.css') as ab:
    st.markdown(f"<style>{ab.read()}</style>", unsafe_allow_html=True)

    
st.logo(logo)
def selected_postcode(postcode):
    return session.table('DEFAULT_SCHEMA.POSTCODES').filter(col('NAME1')==postcode).select('GEOGRAPHY','PC_SECT')


#### SIDE BAR TO CHANGE IRRADIATION SLIDERS

with st.sidebar:
    with st.expander('Adjust % irradation'):
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


st.markdown('<h0black>SOLAR POWER | </h0black><h0blue>ENERGY INSIGHTS</h0blue><BR>', unsafe_allow_html=True)

#### GENERIC TOOLTIP FOR BOTH MAPS

tooltip = {
   "html": """ 
   <br> <b>UPRN:</b> {UPRN} 
   <br><b>Number of Unique Addresses</b> {NUMBER_OF_UNIQUE_ADDRESSES}
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
       "width":"40%",
        "backgroundColor": "#11567F",
        "color": "white",
       "text-wrap": "balance"
   }
}

### ADD WEATHER INFO AND BUILDINGS INFO

BUILDINGS = session.table('DEFAULT_SCHEMA.BUILDINGS_WITH_ROOF_SPECS')
SOLAR_ELEVATION_DF = session.table('DEFAULT_SCHEMA.SOLAR_ELEVATION')

st.markdown('<h1sub>SEARCH FOR BUILDINGS</h1sub><BR>',unsafe_allow_html=True)


##### CHOOSE LOCATION AND DATE FOR WEATHER INFORMATION

col1,col2,col3, col4 = st.columns(4)


with col1:
    filter = BUILDINGS.select('NAME1_TEXT').distinct()
    filter = st.selectbox('Choose Town:',filter, 9)
with col2:
    postcodes_R = session.table('DEFAULT_SCHEMA.POSTCODES')
    postcodes = postcodes_R.filter(col('NAME1_TEXT')==filter)
    postcodef = st.selectbox('Postcode:',postcodes)
    pcode_selected = postcodes_R.filter(col('NAME1')==postcodef).select('PC_SECT')
    
with col3:
    distance = st.number_input('Distance in M:', 20,2000,500)
with col4:
    selected_date = st.date_input('Date for Solar Elevation Angle:',datetime.date(2024,1, 1),datetime.date(2024,1,1),datetime.date(2024,12,31))
st.divider()

##### FILTER WEATHER DATA BASED ON LOCATION AND TIME
SOLAR_ELEVATION_DF = SOLAR_ELEVATION_DF.join(pcode_selected,'PC_SECT')
SOLAR_ELEVATION_DF_FILTERED = SOLAR_ELEVATION_DF.filter(call_function('date',col('"Validity_date_and_time"'))==selected_date)

#### FILTER BUILDINGS BASED ON LOCATION
BUILDINGS = BUILDINGS.filter(col('NAME1_TEXT')==filter)
selected_point = selected_postcode(postcodef)
BUILDINGS = BUILDINGS.join(selected_point.with_column_renamed('GEOGRAPHY','SPOINT'),
                           call_function('ST_DWITHIN',selected_point['GEOGRAPHY'],
                                        BUILDINGS['GEOGRAPHY'],distance)).drop('SPOINT')

#### MAKE BUILDINGS DIRECT IRRADIANCE BASED ON SLOPEYNESS OF ROOFS AND SLIDERS

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


##### PRODUCE SUMMARY TO UNDERSTAND POTENTIAL ENERGY CAPTURE IF ALL BUILDINGS IN SELECTION HAD SOLAR PANELS INSTALLED

SOLAR_BUILDINGS_SUM = BUILDINGS.agg(sum('DIRECT_IRRADIANCE_M2').alias('DIRECT_IRRADIANCE_M2'),
                                   sum('GEOMETRY_AREA_M2').alias('TOTAL_AREA')).join(SOLAR_ELEVATION_DF_FILTERED.group_by('"Validity_date_and_time"').agg(avg('"Solar_elevation_angle"').alias('"Solar_elevation_angle"')))
SOLAR_BUILDINGS_SUM = SOLAR_BUILDINGS_SUM.with_column('total_energy',when(col('"Solar_elevation_angle"')<0,0).otherwise(col('DIRECT_IRRADIANCE_M2')*cos(radians(lit(solar_panel_angle))-col('"Solar_elevation_angle"'))))
                                                                                                     
st.markdown('<h1sub> TOTAL AREA AVAILABLE FOR ENERGY CONVERSION</h1sub>',unsafe_allow_html=True)

### VISUALISE TIME ANALYSIS FOR A PARTICULAR DATE IN THE YEAR

with st.expander('View Time Analysis'):
    st.bar_chart(SOLAR_BUILDINGS_SUM.to_pandas(),y='TOTAL_ENERGY',x='Validity_date_and_time', color='#29B5E8')

st.divider()



### CREATE A VIEW OF BUILDINGS WHICH ALSO INCLUDES COLOURS TO PRODUCE CONDITIONAL FORMATTING WHICH DEPENDS ON EFFICIENCY RATIO

BUILDINGS_V = BUILDINGS.limit(2000).group_by('OSID',
                                            'PRIMARYSITEID',
                                           'THEME',
                                           'DESCRIPTION',
                                           col('GEOMETRY_AREA_M2').astype(StringType()).alias('GE'),
                                           'ROOFMATERIAL_PRIMARYMATERIAL',
                                           'ROOFMATERIAL_SOLARPANELPRESENCE',
                                           'ROOFMATERIAL_GREENROOFPRESENCE',
                                           'ROOFSHAPEASPECT_SHAPE',
                                            col('ROOFSHAPEASPECT_AREAPITCHED_M2').astype(StringType()).alias('A'),
                                            col('ROOFSHAPEASPECT_AREAFLAT_M2').astype(StringType()).alias('RF'),
                                            col('DIRECT_IRRADIANCE_M2').astype(StringType()).alias('D'),
                                            div0(col('D'),col('GEOMETRY_AREA_M2')).alias('EFFICIENCY_RATIO'),
                                            when(col('EFFICIENCY_RATIO')>=0.9,[255,159,54]).when(col('EFFICIENCY_RATIO')>=0.8,[212,91,144]).otherwise([41,181,232]).alias('COLOR'),
                                            col('COLOR')[0].alias('R'),
                                            col('COLOR')[1].alias('G'),
                                            col('COLOR')[2].alias('B'))\
.agg(array_to_string(array_agg('UPRN'),
                     lit(', ')).alias('UPRN'),
     approx_count_distinct('UPRN').alias('NUMBER_OF_UNIQUE_ADDRESSES'),
     any_value('GEOGRAPHY').alias('GEOGRAPHY'))


                                                              
### NEW DATAFRAME TO LIMIT TO THE BUILDING THAT HAS THE MOST AREA AVAILABLE FOR SOLAR PANELS WITH SLOPEYNESS TAKEN INTO ACCOUNT (DIRECT IRRADIANCE IN M2 AND ADD THE CENTROID TO FOCUS A NEW MAP)
z = BUILDINGS_V.with_column('D',(col('D').astype(FloatType()))).sort(col('D').desc()).limit(1).with_column('CENTROID',call_function('ST_CENTROID',col('GEOGRAPHY')))\
.with_column('LON',call_function('ST_X',col('CENTROID')))\
.with_column('LAT',call_function('ST_Y',col('CENTROID'))).drop('CENTROID')

#### FILTER BUILDINGS AND BUILDING PARTS TO PRODUCE DATAFRAMES FOR CORTEX
zosid = z.select('OSID','LAT','LON')
zdet1 = zosid.join(BUILDINGS_V,'OSID')
zdet0 = zosid.join(BUILDINGS,'OSID').drop('GEOMETRY','GEOGRAPHY')
zdet0_grouped = zdet0.group_by(*[col(c) for c in zdet0.columns if c != 'UPRN']) \
                     .agg(count_distinct(col('UPRN')).alias('NUMBER_OF_ADDRESSES'))


#### JOIN THE SITE REF NUMBER IN ORDER TO JOIN TO BUILDING PARTS FOR THE HIGHLIGHTED BUILDING

cross_ref = session.table('SAMPLE_BUILDINGS__GB_OS_NATIONAL_GEOGRAPHIC_DATABASE.PRS_BUILDING_FEATURES_SCH.PRS_BUILDING_BLDTOBLDPRTCROSSREF_TBL')

site_ref = session.table('SAMPLE_BUILDINGS__GB_OS_NATIONAL_GEOGRAPHIC_DATABASE.PRS_BUILDING_FEATURES_SCH.PRS_BUILDING_BLDTOSTECROSSREF_TBL')

site_ref = site_ref.join(cross_ref.drop('BUILDINGVERSIONDATE'),'BUILDINGID')
zdet = zdet1.join(site_ref,site_ref['SITEID']==zdet1['PRIMARYSITEID'])
zparts = session.table('SAMPLE_BUILDINGS__GB_OS_NATIONAL_GEOGRAPHIC_DATABASE.PRS_BUILDING_FEATURES_SCH.PRS_BUILDINGPART_TBL')
zparts = zparts.join(zdet.drop('OSID','GEOGRAPHY',
                               'DESCRIPTION',
                               'THEME','GE',
                              
                              ),zparts['OSID']==zdet['BUILDINGPARTID'])

zparts = zparts.with_column_renamed('GEOMETRY_AREA_M2','GE')






zpd = z.to_pandas()
zpd2 = zparts.select('UPRN',
                     'NUMBER_OF_UNIQUE_ADDRESSES',
                     'GEOGRAPHY',
                     'LAT',
                     'LON',
                     'HEIGHT_ABSOLUTEMIN_M',
                     'GE',
                     'THEME',
                     'DESCRIPTION',
                     'ROOFMATERIAL_PRIMARYMATERIAL',
                     'ROOFSHAPEASPECT_SHAPE',
                     'ROOFMATERIAL_SOLARPANELPRESENCE',
                     'ROOFMATERIAL_GREENROOFPRESENCE','R','G','B','A','RF','D',div0(col('D'),col('GE')).alias('EFFICIENCY_RATIO')).to_pandas()
                     

zpd["coordinates"] = zpd["GEOGRAPHY"].apply(lambda row: json.loads(row)["coordinates"])
zpd2["coordinates"] = zpd2["GEOGRAPHY"].apply(lambda row: json.loads(row)["coordinates"])
zLON = zpd.LON.iloc[0]
zLAT = zpd.LAT.iloc[0]

##### CORTEX PROMPT ENGINEERING

prompt = 'Tell me information about the building as well as the building part information including location information - details specified in the following infomation:'
prompt2 = 'Also tell me about the solar elevation and the weather, and how it might relate to the building.'
wf_json = SOLAR_ELEVATION_DF_FILTERED.drop('POINT').selectExpr("OBJECT_CONSTRUCT(*) AS json_object")
wf_json = wf_json.select(array_agg('JSON_OBJECT').astype(StringType()).alias('WEATHER_OBJECT'))

bf_json = zdet0_grouped.selectExpr("OBJECT_CONSTRUCT(*) AS json_object")
bf_json = bf_json.select(array_agg('JSON_OBJECT').astype(StringType()).alias('BUILDING_OBJECT'))

df_json = zparts.drop('GEOMETRY','GEOGRAPHY').selectExpr("OBJECT_CONSTRUCT(*) AS json_object")
df_json = df_json.select(array_agg('JSON_OBJECT').astype(StringType()).alias('PART_OBJECT'))
data = df_json.join(wf_json).join(bf_json)

cortex = data.select(call_function('snowflake.cortex.complete',
                                          'claude-3-5-sonnet',
                                          concat(lit(prompt),
                                             lit('BUILDING_INFO '),col('BUILDING_OBJECT'),lit('BUILDING_PART'),col('PART_OBJECT'),lit(prompt2),col('WEATHER_OBJECT'),
                                            lit('Return in Markdown, reverse geocode the location where possible and finish with a written summary and use icons to meke it easy to read'))).alias('CORTEX'))



### LAYERS FOR SMALL MAP WITH ONE BUILDING

potential = pdk.Layer(
    "PolygonLayer",
    zpd,
    opacity=1,
    get_polygon="coordinates", 
    filled=True,
    get_fill_color=["R-1","G-1","B-1"],
    get_line_color=[0, 0, 0],
    get_line_width=0.3,
    auto_highlight=True,
    pickable=True,
)

potential_parts = pdk.Layer(
    "PolygonLayer",
    zpd2,
    opacity=0.5,
    get_elevation='HEIGHT_ABSOLUTEMIN_M',
    extruded=True,
    elevation_scale=1,
    get_polygon="coordinates", 
    filled=True,
    get_fill_color=["R-1","G-1","B-1"],
    get_line_color=[0, 0, 0],
    get_line_width=0.3,
    auto_highlight=True,
    pickable=True,
)
zview_state = pdk.ViewState(
    longitude=zLON,
    latitude=zLAT,
    pitch=50,
    zoom=18,  # Adjust zoom if needed
    
)
zr = pdk.Deck(
    layers=[potential,potential_parts],
    initial_view_state=zview_state,
    map_style=None,
    tooltip=tooltip)


### ADD CORTEX AND SIDE MAP TO THE SIDE BAR

with st.sidebar:
    st.divider()
    st.markdown('<h1grey> building with most potential</h1grey>',unsafe_allow_html=True)
    
    st.markdown(f'''<h1grey>DESCRIPTION: <h1sub>{zpd.DESCRIPTION.iloc[0]}</h1sub><BR>
    <h1grey>PRIMARY MATERIAL: </h1grey><h1sub>{zpd.ROOFMATERIAL_PRIMARYMATERIAL.iloc[0]}</h1sub><BR>
    <h1grey>ROOF ASPECT SHAPE: </h1grey><h1sub>{zpd.ROOFSHAPEASPECT_SHAPE.iloc[0]}</h1sub><BR>
    <h1grey>SOLAR PANEL PRESENCE: </h1grey><h1sub>{zpd.ROOFMATERIAL_SOLARPANELPRESENCE.iloc[0]}</h1sub><BR>
    <h1grey>GREEN ROOF PRESENCE: </h1grey><h1sub>{zpd.ROOFMATERIAL_GREENROOFPRESENCE.iloc[0]}</h1sub><BR>
    ''',unsafe_allow_html=True)

    st.markdown(f'<h1sub>CORTEX INSIGHTS</h1sub><br><p>{prompt} <BR><BR> {prompt2}',unsafe_allow_html=True)
    
    with st.container():
        st.markdown(f'<h1sub>RAW DATA CAPTURED</h1sub>',unsafe_allow_html=True)
        with st.expander('Weather Data'):
            st.write(data.collect()[0][1])
        with st.expander('Building Data'):
            st.write(data.collect()[0][2])
        with st.expander('Building Part Data'):
            st.write(data.collect()[0][0])
    run_cortex = st.button('RUN CORTEX')

    if run_cortex:
        cortex = cortex.collect()[0][0]
        with st.expander('Cortex Driven Insights'):
            st.markdown(f'{cortex}',unsafe_allow_html=True)
    
    
    

    st.pydeck_chart(zr, use_container_width=True,height=600)
    st.divider()

    

#### CREATE THE MAIN MAP WITH ALL BUILDINGS

centre = selected_point
centre = centre.with_column('LON',call_function('ST_X',col('GEOGRAPHY')))
centre = centre.with_column('LAT',call_function('ST_Y',col('GEOGRAPHY')))




centrepd = centre.select('LON','LAT').to_pandas()
LON = centrepd.LON.iloc[0]
LAT = centrepd.LAT.iloc[0]
# Populate dataframe from query

datapd = BUILDINGS_V.to_pandas()

datapd["coordinates"] = datapd["GEOGRAPHY"].apply(lambda row: json.loads(row)["coordinates"])

st.markdown('<h1sub>BUILDINGS COLOR CODED BY SOLAR POTENTIAL EFFICIENCY</h1sub>',unsafe_allow_html=True)

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

st.markdown('''|<h1grey> EFFICIENCY RATIO  --->>   </h1sub> | <orange20>MORE THAN  0.9</orange20> | <orange20>■</orange20> | <pink20> between 0.8 and 0.9 </pink20>| <pink20>■</pink20> | <blue20>less than 0.8  </blue20> | <blue20>■</blue20> |''', unsafe_allow_html=True)
    
st.pydeck_chart(r, use_container_width=True,height=700)

st.caption('Colour key for the buildings.  Efficiency Ratio is the total area divided by the solar coverage lost by the pitch of each roof')
