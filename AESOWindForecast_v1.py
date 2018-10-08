# -*- coding: utf-8 -*-
"""
Created on Sat Oct  6 16:04:08 2018

@author: Andrew Yan

#ST_WIND_FORECAST_URL needs to be scraped every 10 minutes.
#LT_WIND_FORECAST_URL needs to be scraped every 6 hours
"""

import pandas as pd
import numpy as np
import re
import datetime
import psycopg2
import psycopg2.extras
import csv
import requests

#how to use the lib below
from sqlalchemy import create_engine


#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#set up display
pd.set_option('display.width',1000)
pd.set_option('display.max_rows',100)
pd.set_option('display.max_columns',20)


#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#set up variables and queries
QUERY_INSERT_WIND = "INSERT INTO public.wind_forecast (aeso_report_time, script_update_time, forecast_transaction_date, forecasted_min, forecasted_most_likely, forecasted_max) VALUES(%s,%s,%s, %s, %s, %s );"

WIND_FORECAST_COLUMNS=['aeso_report_time', 'script_update_time','forecast_transaction_date', 'forecasted_min', 'forecasted_most_likely', 'forecasted_max']

#Short-term wind forecast displays data on a 12-hour ahead basis in hourly intervals and is updated every 10 minutes. It is based on the currently installed capacity listed on AESO Current Supply and Demand page
ST_WIND_FORECAST_URL = "http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_power_forecast/WPF_ShortTerm.csv"

#AESO long term wind power forecast uses global weather data to indicate the amount that will be available to the Alberta grid on a seven-day ahead basis, and is updated every six hours.
LT_WIND_FORECAST_URL = "http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_power_forecast/WPF_LongTerm.csv"

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#Connect to the database
conn = psycopg2.connect(dbname='power', user='postgres', host='149.28.238.141', password="xiaoyu03")
cur = conn.cursor()   

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#functions below
def get_st_wind_data(WIND_FORECAST_URL):
    '''
    Get data from AESO wind forecast URLs and insert into database,
    you have to run this function for ST_WIND_FORECAST_URL and LT_WIND_FORECAST_URL separately.
    '''
    
    with requests.Session() as s:
        
        download = s.get(WIND_FORECAST_URL)
        
        #record the script timestamp in utc time
        script_update_time=datetime.datetime.utcnow()
        
        decoded_content = download.content.decode('utf-8')
        
        cr = csv.reader(decoded_content.splitlines(), delimiter = ',')
        
    wind_forecast_data = list(cr)
          
    # find datetime and convert it into datetime
    #'Alberta 12 Hour Wind Power forecast updated as of 10/7/2018  11:54:00AM MT'
    last_updated=wind_forecast_data[0]
    last_updated=last_updated[0][50:]
    last_updated=re.sub(r" MT",r"",last_updated)
    aeso_report_time=pd.to_datetime(last_updated)
    #print("Found AESO last update time: {}".format(aeso_report_time))
    
    #put the wind forecast data into a dataframe called wind_forecast_table
    #the data for both ST_WIND_FORECAST_URL and LT_WIND_FORECAST_URL end in the last 3rd low, therefore
    #the data range is set using "wind_forecast_data[3:-2]"
    wind_forecast_table = pd.DataFrame.from_records(wind_forecast_data[3:-2],columns=['forecast_transaction_date', 'forecasted_min', 'forecasted_most_likely', 'forecasted_max'])
    
    
    #create a table called result and insert the required data into it and send insert it to the database
    result = pd.DataFrame(columns=WIND_FORECAST_COLUMNS)
    

    for i in range(0,len(wind_forecast_table.index)):
                             
        forecast_transaction_date = wind_forecast_table.iloc[i].at['forecast_transaction_date']
        forecasted_min = wind_forecast_table.iloc[i].at['forecasted_min']
        forecasted_most_likely = wind_forecast_table.iloc[i].at['forecasted_most_likely']
        forecasted_max = wind_forecast_table.iloc[i].at['forecasted_max']
        
        data = [aeso_report_time, script_update_time,forecast_transaction_date, forecasted_min, forecasted_most_likely, forecasted_max]

        result = result.append(pd.Series(data, index=WIND_FORECAST_COLUMNS), ignore_index=True)
       
        count=i+1
        
        cur.execute(QUERY_INSERT_WIND, data)

    #need to work on the try and except part below            
    try:
        conn.commit()
        print("Inserted {} lines of wind forecast data".format(count))
    except IntegrityError:
        print("data already exists in database")
        
    conn.close()
    
    return (result)

get_st_wind_data(LT_WIND_FORECAST_URL)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
