# Import python packages
import streamlit as st
from streamlit import session_state as S
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
import datetime
from snowflake.snowpark import types as T
from snowflake.snowpark.window import Window
import altair as alt
import matplotlib.pyplot as plt
import geopandas as gpd
from matplotlib.colors import LinearSegmentedColormap, ListedColormap


# Get the current credentials
session = get_active_session()

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
    .blue {
        color: rgb(41, 181, 232);  /* windy city */
        font-size: 24px;
        font-weight: bold;
    
    div[role="tablist"] > div[aria-selected="true"] {
        background-color: rgb(41, 181, 232);
        color: rgb(0,53,69);  /* Change the text color if needed */
    }
    
    </style>
    """,
    unsafe_allow_html=True
)

st.logo('https://upload.wikimedia.org/wikipedia/commons/f/ff/Snowflake_Logo.svg')
st.markdown(
        f"""
            <style>
                [data-testid="stSidebar"] {{
                    background-color: white;
                    background-repeat: no-repeat;
                    padding-top: 20px;
                    background-position: 20px 20px;
                }}
            </style>
            """,
        unsafe_allow_html=True,
    )


try:

    with st.sidebar:

        SCENARIO = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').select('SCENARIO').distinct()
    
    
        SELECT_SCENARIO = st.selectbox('Choose Scenario: ', SCENARIO)
 

    
        clear = st.button('Clear All Scenarios')

        if clear:
            session.sql(f'DROP TABLE DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').collect()



    

        data = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        st.markdown(f'''Start Date: {data.select('Options: Start Date').distinct().to_pandas()['Options: Start Date'].iloc[0]}''')
        st.markdown(f'''Start Date: {data.select('Options: Start Date').distinct().to_pandas()['Options: Start Date'].iloc[0]}''')
        st.markdown(f'''Description: {data.select('Scenario Description').distinct().to_pandas()['Scenario Description'].iloc[0]}''')
        st.markdown(f'''Weekly Amount: {data.select('Options: Weekly Amount £').distinct().to_pandas()['Options: Weekly Amount £'].iloc[0]}''')
        st.markdown(f'''Temperature Threshold: {data.select('Options: Temperature Threshold').distinct().to_pandas()['Options: Temperature Threshold'].iloc[0]}''')
        st.markdown(f'''Average X Days: {data.select('Options: Average X Days').distinct().to_pandas()['Options: Average X Days'].iloc[0]}''')
        st.markdown(f'''Weather Measure: {data.select('Options: Weather Measure').distinct().to_pandas()['Options: Weather Measure'].iloc[0]}''')




    st.markdown('<h1 class="heading">SCENARIO ANALYSIS</h2><BR>', unsafe_allow_html=True)

    

    st.markdown('<p class="blue">AREAS AFFECTED BY THE SCENARIOS</p>', unsafe_allow_html=True)
    col1,col2,col3 = st.columns([0.3,0.4,0.3])

    @st.cache_data
    def geos1():
        
        return session.table(f'{S.shareddata}.COLD_WEATHER_PAYMENTS."Postal Out Code Geometries"').to_pandas()

    @st.cache_data
    def geos2(SELECT_SCENARIO):
        data = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        geos = session.table(f'{S.shareddata}.COLD_WEATHER_PAYMENTS."Postal Out Code Geometries"')
        return geos.join(data,on=geos['"Name"']==data['"Postcode Area"'],lsuffix='L').to_pandas()
        
    with col2:
        geos = session.table(f'{S.shareddata}.DATA."Postal Out Code Geometries"')
        geo_with_data = geos
        geom = geos1()
        geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe.crs = "EPSG:4326"

        #geo_with_data = geos.join(data,on=geos['"Name"']==data['"Postcode Area"'],lsuffix='L')
        #geom = geo_with_data.to_pandas()
        geom = geos2(SELECT_SCENARIO)

        geodframe2 = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe2.crs = "EPSG:4326"



        fig, ax = plt.subplots(1, figsize=(10, 5))
        ax.axis('off')
        geodframe.crs = "EPSG:4326"
        geodframe.plot(color='white',alpha=0.8,ax=ax, edgecolors='grey',linewidth=0.2, figsize=(9, 10))
        geodframe2.plot(color='#29b5e8', alpha=1,ax=ax, figsize=(9, 10))
        st.pyplot(fig)


    measure = st.selectbox('Choose Measure:',['Adults','Number of Households','Under 16s','Min Temp','Total Payable'])
    col1,col2 = st.columns(2)

    @st.cache_data
    def data_for_charts(SELECT_SCENARIO):
        data = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO').filter(F.col('SCENARIO')==SELECT_SCENARIO)
        return data.to_pandas()
    
    with col1:

        
        d = alt.Chart(data_for_charts(SELECT_SCENARIO)).mark_bar(color='#29b5e8').encode(
        alt.X('Postcode Area:N', sort=alt.EncodingSortField(field=measure,order='descending')),
        alt.Y(f'{measure}:Q'))

        st.altair_chart(d, use_container_width=True)
    with col2:
        c = (alt.Chart(data_for_charts(SELECT_SCENARIO)).mark_circle(color='#29b5e8').encode(
                    x='Min Temp',
                    y= measure,
                    #color= alt.color('#31b1c9'),
                    tooltip=['Postcode Area', 'Min Temp'])
    ).interactive()

        st.altair_chart(c, use_container_width=True)

        

        #st.markdown('#### Summary Data')

        #st.dataframe(data)
    
except:
    st.write('No Data Found - Please Create a Scenario first')