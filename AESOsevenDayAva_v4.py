# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re
import datetime
import psycopg2
import psycopg2.extras

#how to use the lib below
from sqlalchemy import create_engine


#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#set up display
pd.set_option('display.width',1000)
pd.set_option('display.max_rows',100)
pd.set_option('display.max_columns',10)


#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#set up variables and queries
QUERY_INSERT_MC = "INSERT INTO public.seven_day_mc (aeso_report_time, script_update_time,coal, gas, hydro, wind, other) VALUES(%s,%s,%s, %s, %s, %s, %s);"
QUERY_INSERT_BASE = "INSERT INTO public.seven_day_{} (forecast_date, hour_ending, availability, aeso_report_time, script_update_time) VALUES(%s, %s, %s, %s, %s);"
DB_COLUMNS = ['forecast_date', 'hour_ending', 'availability', 'aeso_report_time', 'script_update_time']
TB_MC_COLUMNS=['aeso_report_time', 'script_update_time','coal', 'gas', 'hydro', 'wind', 'other']
DATA_URL = "http://ets.aeso.ca/ets_web/ip/Market/Reports/SevenDaysHourlyAvailableCapabilityReportServlet?contentType=html"
FUEL_TYPE = ['coal', 'gas', 'hydro', 'wind', 'other']



#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#functions below
def formatData(data):
    '''
    The formatData() function takes in a table and return each fuel type's manufacturing capacity and the availabity in float data.
    '''
    mc = int(re.sub(r".*\(MC\xa0= |\xa0MW\xa0\)", '', data.iloc[0][0]))
    data.iloc[0] = data.iloc[0].T.shift(-1).T
    data = data.drop(columns=25)
    data[0] = pd.to_datetime(data[0])
    data.set_index(0, inplace=True)
    data.index.name = 'forecast_date'
    data = data.apply(lambda x: x.str.rstrip('%')).astype('float') / 100
    #add the aeso_report_time
    #data["aeso_report_time"]=aeso_report_time
    #add the UTC time to record scrape timestamp
    #data["script_update_time"]=script_update_time
    return mc, data

def extractData(source, row_start, row_end):
    '''
    taking a table with info such as its beginning of row # and ending row # and put this info into the formatData() function
    '''
    return formatData(source[row_start:row_end])

def extractAesoUpdateTime(tables):
    '''
    extract the AESO report's last updated time from one of the html tables
    '''
    last_updated=tables[1]
    # find datetime pattern like "2018/10/04 11:36:19"
    date_time_pattern=re.compile(r"\d{4}[\/]\d{2}[\/]\d{2}[\s{1}]\d{2}:\d{2}:\d{2}")
    last_updated_match_result=date_time_pattern.search(str(last_updated[0])).group()
    #convert the datetime found to pandas datetime type data
    aeso_report_time=pd.to_datetime(last_updated_match_result)
    print("Found AESO last update time: {}".format(aeso_report_time))
    return aeso_report_time

def fetchData(url):
    '''
    store the url html data into a table and return the table, aeso report update time, and the script time
    '''
    tables=pd.read_html(url)
    dataTable = tables[2]
    aeso_report_time = extractAesoUpdateTime(tables)
    script_update_time=datetime.datetime.utcnow()
    
    return (dataTable, aeso_report_time, script_update_time)

def inserToDb(fuel_type,data):
    '''isnert the data into the database
    '''
    cur.execute(QUERY_INSERT_BASE.format(fuel_type), data)

def convertAndInsertToDbSchema(fuel_type, data):
    '''
    
    '''
    result = pd.DataFrame(columns=DB_COLUMNS)
    for index, row in data.iterrows():
        forecast_date = index
        #would the line below still work if I replace hour_ending and availability with a and b respectively?
        for hour_ending, availability in row.items():
            data = [forecast_date, hour_ending, availability, aeso_report_time, script_update_time]
            result = result.append(pd.Series(data, index=DB_COLUMNS), ignore_index=True)
            inserToDb(fuel_type,data)
    conn.commit()
    return result


def updateAllFuelTypeAv(data):
    '''
    update the database tables for all fuel types
    '''
    result={}
    
    for fuel_type in FUEL_TYPE:
        print("start to insert {} data into database".format(fuel_type))
        result[fuel_type] = convertAndInsertToDbSchema(fuel_type, data[fuel_type])
        print("finished {} data input".format(fuel_type))
    
    return result
      



def create_fuel_type_tables(dataTable,aeso_report_time,script_update_time):
    '''
    create tables for each fuel type
    '''
    
    fuel_type_av_table = {}
    
    #add aeso_report_time and aeso_report_time to the mc dict
    mc = {"aeso_report_time":aeso_report_time,"script_update_time":script_update_time}

    mc['coal'], fuel_type_av_table['coal'] = extractData(dataTable,1,8)
    mc['gas'], fuel_type_av_table['gas'] = extractData(dataTable,9,16)
    mc['hydro'], fuel_type_av_table['hydro'] = extractData(dataTable,17,24)
    mc['wind'], fuel_type_av_table['wind'] = extractData(dataTable,25,32)
    mc['other'], fuel_type_av_table['other'] = extractData(dataTable,33,40)
    
    return fuel_type_av_table, mc



def convert_and_Insert_To_MC(data):
    '''
    extract values from series called generation_mc_table and put all the variables into a list.
    Then insert these variables into the database
    '''
    
    aeso_report_time = generation_mc_table.loc[0].at['aeso_report_time']
    script_update_time= generation_mc_table.loc[0].at['script_update_time']
    coal= generation_mc_table.loc[0].at['coal']
    gas= generation_mc_table.loc[0].at['gas']
    hydro = generation_mc_table.loc[0].at['hydro']
    wind = generation_mc_table.loc[0].at['wind']
    other = generation_mc_table.loc[0].at['other']
    
    data=[aeso_report_time, script_update_time,coal, gas, hydro, wind, other]
    
    print("start to insert MC data into database")
    cur.execute(QUERY_INSERT_MC, data)
       
    conn.commit()
    
    print("MC data updated")
    
    return data

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#Connect to the database
conn = psycopg2.connect(dbname='power', user='postgres', host='149.28.238.141', password="xiaoyu03")
cur = conn.cursor()   


#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#fetch data from AESO website
dataTable, aeso_report_time, script_update_time = fetchData(DATA_URL)

fuel_type_av_table,mc = create_fuel_type_tables(dataTable,aeso_report_time,script_update_time)



#create a DataFrame from  mc
generation_mc_table=pd.DataFrame(pd.Series(mc)).T



#insert data in to the "seven_day_mc" table in database "Power "
convert_and_Insert_To_MC(generation_mc_table)



#insert 7-day availability by fuel type data into each fuel type tables     
aeso_avalibility_by_fuel_type = updateAllFuelTypeAv(fuel_type_av_table)



#close the connection to the database
conn.close()
print("7 day fuel type data has been updated")    
    
        
#-----------------------------------------------------------------------------------------------------------------------------------------------------------
# Additional things need to study
       
# ====== Connection ======
# Connecting to PostgreSQL by providing a sqlachemy engine
#engine = create_engine('postgresql://'+os.environ['POSTGRESQL_USER']+':'+os.environ['POSTGRESQL_PASSWORD']+'@'+os.environ['POSTGRESQL_HOST_IP']+':'+os.environ['POSTGRESQL_PORT']+'/'+os.environ['POSTGRESQL_DATABASE'],echo=False)



'''
def convert_and_Insert_To_MC(generation_mc_table):
    result = pd.DataFrame(columns=TB_MC_COLUMNS)
    for index, row in generation_mc_table.iterrows():
        aeso_report_time = index
        for coal, coal_capacity in row.items():
            generation_mc_table = [aeso_report_time, script_update_time,coal, gas, hydro, wind, other]
            result = result.append(pd.Series(generation_mc_table, index=TB_MC_COLUMNS), ignore_index=True)
            insert_MC(generation_mc_table)
    conn.commit()
    return result

'''










