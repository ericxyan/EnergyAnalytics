# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re
import datetime
import time
import psycopg2
import psycopg2.extras

pd.set_option('display.width',1000)
pd.set_option('display.max_rows',100)
pd.set_option('display.max_columns',10)

query = "INSERT INTO public.seven_day_coal (forecast_date, hour_ending, availability, aeso_report_time, script_update_time) VALUES(%s, %s, %s, %s, %s);"
db_columns=['forecast_date', 'hour_ending', 'availability', 'aeso_udpated_time', 'script_update_time']
url = "http://ets.aeso.ca/ets_web/ip/Market/Reports/SevenDaysHourlyAvailableCapabilityReportServlet?contentType=html"

def formatData(data):
    mc = int(re.sub(r".*\(MC\xa0= |\xa0MW\xa0\)", '', data.iloc[0][0]))
    data.iloc[0] = data.iloc[0].T.shift(-1).T
    data = data.drop(columns=25)
    data[0] = pd.to_datetime(data[0])
    data.set_index(0, inplace=True)
    data.index.name = 'forecast_date'
    data = data.apply(lambda x: x.str.rstrip('%')).astype('float') / 100
    #add the aeso_udpated_time
    #data["aeso_report_time"]=aeso_udpated_time
    #add the UTC time to record scrape timestamp
    #data["script_update_time"]=script_update_time
    return mc, data

def extractData(source, row_start, row_end):
    return formatData(source[row_start:row_end])

def extractAesoUpdateTime(tables):
    last_updated=tables[1]
    # find datetime pattern like "2018/10/04 11:36:19"
    date_time_pattern=re.compile(r"\d{4}[\/]\d{2}[\/]\d{2}[\s{1}]\d{2}:\d{2}:\d{2}")
    last_updated_match_result=date_time_pattern.search(str(last_updated[0])).group()
    #convert the datetime found to pandas datetime type data
    aeso_udpated_time=pd.to_datetime(last_updated_match_result)
    print("Found AESO last update time: {}".format(aeso_udpated_time))
    return aeso_udpated_time

def fetchData(url):
    tables=pd.read_html(url)
    dataTable = tables[2]
    aeso_udpated_time = extractAesoUpdateTime(tables)
    script_update_time=datetime.datetime.utcnow()
    
    return (dataTable, aeso_udpated_time, script_update_time)

def inserToDb(data):
    cur.execute(query, data)

def convertAndInsertToDbSchema(powerType, data):
    result = pd.DataFrame(columns=db_columns)
    for index, row in data.iterrows():
        forecast_date = index
        for hour_ending, availability in row.items():
            data = [forecast_date, hour_ending, availability, aeso_udpated_time, script_update_time]
            result = result.append(pd.Series(data, index=db_columns), ignore_index=True)
            inserToDb(data)
    conn.commit()
    return result


conn = psycopg2.connect(dbname='power', user='postgres', host='149.28.238.141', password="xiaoyu03")
cur = conn.cursor()   


dataTable, aeso_udpated_time, script_update_time = fetchData(url)


#add aeso_udpated_time and aeso_report_time to the mc dict
mc = {"aeso_udpated_time":aeso_udpated_time,"script_update_time":script_update_time}
data = {}

mc['coal'], data['coal'] = extractData(dataTable,1,8)
mc['gas'], data['gas'] = extractData(dataTable,9,16)
mc['hydro'], data['hydro'] = extractData(dataTable,17,24)
mc['wind'], data['wind'] = extractData(dataTable,25,32)
mc['other'], data['other'] = extractData(dataTable,33,40)


#create a DataFrame from  mc
generation_capacity_table=pd.DataFrame(pd.Series(mc)).T

print(data['coal'])

print(generation_capacity_table)

#connect to database

db_table = {}
db_table['coal'] = convertAndInsertToDbSchema('coal', data['coal'])














