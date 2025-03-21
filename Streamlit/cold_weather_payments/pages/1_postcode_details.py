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

#load logo

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

    .blue {
        color: rgb(41, 181, 232);  /* windy city */
        font-size: 24px;
        font-weight: bold;
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

st.markdown('<h1 class="heading">POSTCODE DETAILS</h2><BR>', unsafe_allow_html=True)
try:
    population = session.table(f'{S.shareddata}.COLD_WEATHER_PAYMENTS."Synthetic Population"')
    #only_not_working
    population_not_working = population.filter(F.col('OCCUPATION_CODE')==2)
    #exclude children and not working 
    population_working = population.filter((F.col('OCCUPATION_CODE')!=2)&(F.col('OCCUPATION_CODE')!=1))
    children = population.filter((F.col('OCCUPATION_CODE')==1))

    household_children = children.group_by(F.col('HOUSEHOLD'),F.col('POSTCODE')).agg(F.count('*').alias('Children'))

    #working household
    working_household = population_working\
                 .select('HOUSEHOLD','NI NUMBER')\
                 .group_by(F.col('HOUSEHOLD'))\
                 .agg(F.count('*').alias('WORKING_PEOPLE'))

    #those entitled to coldweather_payments

    households_cold_weather = S.households_cold_weather.join(household_children,on='HOUSEHOLD',how='outer',rsuffix='C')

    households_cold_weather=S.households_cold_weather.group_by('POSTCODE_AREA').agg(F.sum('Adults').alias('"Adults"'), 
                                                                              F.count('*').alias('Number of Households'),F.count('Children').alias('Under 16s'))







    #st.dataframe(entitled_payments)
    col1,col2,col3,col4 = st.columns(4)

    with col1:
        st.metric('Payments - £Mil: ', 
            round(S.entitled_payments.agg(F.sum('Total Payable').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000000,2))

    with col2:
        st.metric('Households - Thous: ', 
            round(S.entitled_payments.agg(F.sum('"Number of Households"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000,2))

    with col3:
        st.metric('Adults - Thous: ', 
            round(S.entitled_payments.agg(F.sum('"Adults"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000,2))

    with col4:
        st.metric('Under 16s - Thous: ', 
            round(S.entitled_payments.agg(F.sum('"Under 16s"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0]/1000,2))



    st.divider()



   
    st.markdown(f'<p class="blue">POSTCODE AREAS AFFECTED BY THE PAYMENT POLICY</p>', unsafe_allow_html=True)

    joined = S.entitled_payments.join(S.geos,on=S.entitled_payments['"Postcode Area"']==S.geos['"Name"'],lsuffix='_e')

    joined = joined.drop('LAT_E','LON_E')
        #st.dataframe(joined.columns)






    st.divider()

    st.markdown(f'<p class="blue">POSTCODE SECTOR DETAILS</p>', unsafe_allow_html=True)
    S.select_description = st.selectbox('Select Postcode Area:',joined.select('"Name"'))


    entitled_payments = S.entitled_payments.filter(F.col('"Postcode Area"')==S.select_description)

    col1,col2,col3,col4 = st.columns(4)

    with col1:
        st.metric('Payments - £Thous: ', 
        round(S.entitled_payments.agg(F.sum('Total Payable').alias('TOTAL')).to_pandas()\
                .TOTAL.iloc[0]/1000,2))

    with col2:
        st.metric('Households - Thous: ', 
        round(S.entitled_payments.agg(F.sum('"Number of Households"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0],2))

    with col3:
        st.metric('Adults Thous: ', 
        round(S.entitled_payments.agg(F.sum('"Adults"').alias('TOTAL')).to_pandas()\
                    .TOTAL.iloc[0],2))

    with col4:
        st.metric('Under 16s - Thous: ', 
        round(S.entitled_payments.agg(F.sum('"Under 16s"').alias('TOTAL')).to_pandas()\
                .TOTAL.iloc[0]/1000,2))






    col1,col2=st.columns([0.6,0.4])

    with col1:
        geo_filtered = S.geos.filter(F.col('"Name"')==S.select_description)
        geom = gpd.GeoDataFrame(geo_filtered.to_pandas())
    
        geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
        geodframe.crs = "EPSG:4326"
        fig, ax = plt.subplots(1, figsize=(10, 5))
        ax.axis('off')
        geodframe.crs = "EPSG:4326"
        geodframe.plot( color = '#29b5e8',ax=ax, figsize=(9, 10))
        st.pyplot(fig)


    with col2:
        all_people =  S.population_entitled_cold_weather.union(S.children)

        filtered_all = S.population_entitled_cold_weather.filter(F.split(F.col('POSTCODE'), F.lit(' '))[0]==S.select_description)

        age_histo = filtered_all.select(F.call_function('WIDTH_BUCKET',
                                                                    F.col('AGE'),
                                                                    0,110,7).alias('Age Group')
                                                    ,'AGE').group_by('Age Group').agg(F.count('*').alias('Total People'),F.cast(F.min('AGE'),T.StringType()).alias('Min Age'),
                                                                                      F.cast(F.max('AGE'),T.StringType()).alias('Max Age'))
        age_histo = age_histo.with_column('Age Range',F.concat(F.col('Min Age'),F.lit('-'),F.col('Max Age')))

        st.markdown('Number of Adult Occupants who would receive cold weather payments')
        st.bar_chart(age_histo,x='Age Range',y='Total People', color = '#29b5e8')

    weather_filter = S.hourly_with_date_ws.filter(F.col('POSTCODE_AREA')==S.select_description)\
    .with_column('AVERAGE_TEMP',F.cast('AVERAGE_TEMP',T.FloatType()))

    st.markdown(f'<p class="blue">WEATHER IN CHOSEN LOCATION</p>', unsafe_allow_html=True)
    st.bar_chart(weather_filter,x='Date',y='AVERAGE_TEMP', color='#29b5e8')

except:
    st.info('Please create a scenario before proceeding')