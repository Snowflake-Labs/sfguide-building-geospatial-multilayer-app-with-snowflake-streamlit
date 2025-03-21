# Import python packages
import streamlit as st
from streamlit import session_state as S
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
import datetime
from snowflake.snowpark import types as T
from snowflake.snowpark.window import Window
import altair as alt

# Get the current credentials
session = get_active_session()
st.set_page_config(layout="wide")

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
    }
    div[role="tablist"] > div[aria-selected="true"] {
        background-color: rgb(41, 181, 232);
        color: rgb(0,53,69);  /* Change the text color if needed */
    }
    
    </style>
    """,
    unsafe_allow_html=True)


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




#st.write(session.get_current_role())




def barchart(data,measure):
    d = alt.Chart(data).mark_bar(color='#29b5e8').encode(
    alt.X('Postcode Area:N', sort=alt.EncodingSortField(field=measure,order='descending')),
        alt.Y(f'{measure}:Q'))
    return st.altair_chart(d, use_container_width=True)





st.markdown('<h1 class="heading">AFFECTED PEOPLE IN SCENARIO</h2><BR>', unsafe_allow_html=True)


SCENARIOS = session.table(f'DATA.{st.experimental_user.user_name}_COLD_WEATHER_PAYMENT_SCENARIO')
SCENARIO = SCENARIOS.select('SCENARIO').distinct()
Postcode = SCENARIOS.select('"Postcode Area"').distinct()


chosen = st.selectbox('Choose Scenario:',SCENARIO)




SELECTEDsp = SCENARIOS.filter(F.col('SCENARIO')== chosen)
SELECTED = SELECTEDsp.to_pandas()

st.markdown(f'''<p class="blue">SCENARIO COMMENTS: {SELECTED['Scenario Description'].iloc[0].upper()}</p>''', unsafe_allow_html=True)


col1,col2,col3,col4,col5 = st.columns(5)


with col1:
    usix = SELECTEDsp.sort(F.col('"Adults"').desc()).limit(5).to_pandas()
    barchart(usix,'Adults')

with col2:
    usix = SELECTEDsp.sort(F.col('"Under 16s"').desc()).limit(5).to_pandas()
    barchart(usix,'Under 16s')

with col3:
    usix = SELECTEDsp.sort(F.col('"Min Temp"').desc()).limit(5).to_pandas()
    barchart(usix,'Min Temp')

with col4:
    usix = SELECTEDsp.sort(F.col('"Total Payable"').desc()).limit(5).to_pandas()
    barchart(usix,'Total Payable')

with col5:
    usix = SELECTEDsp.sort(F.col('"Number of Households"').desc()).limit(5).to_pandas()
    barchart(usix,'Number of Households')


chosen2 = st.selectbox('Choose Postcode:',Postcode)



st.markdown(f'<p class="blue">PEOPLE ENTITLED TO COLD WEATHER PAYMENTS IN ABOVE SCENARIO</p>', unsafe_allow_html=True)


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
population_entitled_cold_weather = population_not_working.join(working_household, on=(population_not_working['HOUSEHOLD']==working_household['HOUSEHOLD']), how='outer',rsuffix='_L').drop('HOUSEHOLD_L')\
    .filter(F.col('WORKING_PEOPLE').isNull()).drop('WORKING_PEOPLE')

filtered_pop = population_entitled_cold_weather.filter(F.split(F.col('POSTCODE'),F.lit(' '))[0]==chosen2)

st.write(filtered_pop.limit(10))