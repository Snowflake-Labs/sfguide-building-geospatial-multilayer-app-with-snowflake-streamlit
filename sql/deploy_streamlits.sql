use role ACCOUNTADMIN;
create database if not exists ANALYSE_LOCATION_DATA;
create schema if not exists ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS;

CREATE STAGE IF NOT EXISTS ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1 DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
CREATE STAGE IF NOT EXISTS ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2 DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
CREATE STAGE IF NOT EXISTS ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3 DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
CREATE STAGE IF NOT EXISTS ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4 DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
------put streamlit files in stages
PUT file:///streamlit/road_network/towns_with_roads.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1 auto_compress = false overwrite = true;
PUT file:///streamlit/road_network/environment.yml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1 auto_compress = false overwrite = true;
PUT file:///streamlit/road_network/config.toml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1/.streamlit auto_compress = false overwrite = true;
PUT file:///streamlit/snowflake_logo_color_rgb.svg @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1/ auto_compress = false overwrite = true;
PUT file:///streamlit/extra.css @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1/ auto_compress = false overwrite = true;

-------put streamlit 2 in stage
PUT file:///streamlit/Slopey_Roofs/slopey_roofs.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2 auto_compress = false overwrite = true;
PUT file:///streamlit/Slopey_Roofs/environment.yml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2 auto_compress = false overwrite = true;
PUT file:///streamlit/Slopey_Roofs/config.toml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2/.streamlit auto_compress = false overwrite = true;
PUT file:///streamlit/snowflake_logo_color_rgb.svg @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2/ auto_compress = false overwrite = true;
PUT file:///streamlit/stylesheets/extra.css @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2/ auto_compress = false overwrite = true;

-------put streamlit 3 in stage
PUT file:///streamlit/cold_weather_payments/Home.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3 auto_compress = false overwrite = true;
PUT file:///streamlit/cold_weather_payments/pages/1_postcode_details.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/pages auto_compress = false overwrite = true;
PUT file:///streamlit/cold_weather_payments/pages/2_view_scenarios.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/pages auto_compress = false overwrite = true;
PUT file:///streamlit/cold_weather_payments/pages/3_scenario_comparisons.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/pages auto_compress = false overwrite = true;
PUT file:///streamlit/cold_weather_payments/pages/4_people_entitled.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/pages auto_compress = false overwrite = true;
PUT file:///streamlit/cold_weather_payments/environment.yml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3 auto_compress = false overwrite = true;
PUT file:///streamlit/cold_weather_payments/config.toml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/.streamlit auto_compress = false overwrite = true;
PUT file:///streamlit/snowflake_logo_color_rgb.svg @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/ auto_compress = false overwrite = true;
PUT file:///streamlit/stylesheets/extra.css @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3/ auto_compress = false overwrite = true;

-------put streamlit 4 in stage
PUT file:///streamlit/flood_zones/flood_zone.py @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4 auto_compress = false overwrite = true;
PUT file:///streamlit/flood_zones/environment.yml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4 auto_compress = false overwrite = true;
PUT file:///streamlit/flood_zones/config.toml @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4/.streamlit auto_compress = false overwrite = true;
PUT file:///streamlit/snowflake_logo_color_rgb.svg @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4/ auto_compress = false overwrite = true;
PUT file:///streamlit/stylesheets/extra.css @ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4/ auto_compress = false overwrite = true;
-----CREATE STREAMLITS

CREATE OR REPLACE STREAMLIT ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.ROAD_NETWORK
ROOT_LOCATION = '@ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT1'
MAIN_FILE = 'towns_with_roads.py'
QUERY_WAREHOUSE = 'LOCATION_ANALYTICS'
COMMENT = '{"origin":"sf_sit", "name":"TOWNS_WITH_ROADS", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":0, "source":"streamlit"}}';

CREATE OR REPLACE STREAMLIT ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.SOLAR_ENERGY_INSIGHTS
ROOT_LOCATION = '@ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT2'
MAIN_FILE = 'slopey_roofs.py'
QUERY_WAREHOUSE = 'LOCATION_ANALYTICS'
COMMENT = '{"origin":"sf_sit", "name":"SLOPEY_ROOFS", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":0, "source":"streamlit"}}';

CREATE OR REPLACE STREAMLIT ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.COLD_WEATHER_PAYMENTS
ROOT_LOCATION = '@ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT3'
MAIN_FILE = 'Home.py'
QUERY_WAREHOUSE = 'LOCATION_ANALYTICS'
COMMENT = '{"origin":"sf_sit", "name":"SLOPEY_ROOFS", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":0, "source":"streamlit"}}';


CREATE OR REPLACE STREAMLIT ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.FLOOD_ZONES
ROOT_LOCATION = '@ANALYSE_LOCATION_DATA.LOCATION_ANALYTICS.STREAMLIT4'
MAIN_FILE = 'flood_zone.py'
QUERY_WAREHOUSE = 'LOCATION_ANALYTICS'
COMMENT = '{"origin":"sf_sit", "name":"SLOPEY_ROOFS", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":0, "source":"streamlit"}}';