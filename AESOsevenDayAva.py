# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

pd.set_option('display.width',1000)
pd.set_option('display.max_rows',100)
pd.set_option('display.max_columns',5)

def formatData(data):
    mc = int(re.sub(r".*\(MC\xa0= |\xa0MW\xa0\)", '', data.iloc[0][0]))
    data.iloc[0] = data.iloc[0].T.shift(-1).T
    data = data.drop(columns=25)
    data[0] = pd.to_datetime(data[0])
    data.set_index(0, inplace=True)
    data.index.name = 'date'
    data = data.apply(lambda x: x.str.rstrip('%')).astype('float') / 100
    return mc, data

def extractData(source, row_start, row_end):
    return formatData(source[row_start:row_end])

tables=pd.read_html("http://ets.aeso.ca/ets_web/ip/Market/Reports/SevenDaysHourlyAvailableCapabilityReportServlet?contentType=html")

dataTable = tables[2]

mc = {}
data = {}
mc['coal'], data['coal'] = extractData(dataTable,1,8)
mc['gas'], data['gas'] = extractData(dataTable,9,16)
mc['hydro'], data['hydro'] = extractData(dataTable,17,24)
mc['wind'], data['wind'] = extractData(dataTable,25,32)
mc['other'], data['other'] = extractData(dataTable,33,40)




#Creating a new column called "fuel_type", fill the value with data from 2nd row " COAL(MC = 5723 MW )" and insert into the 1st column

'''
#Delete the 1st row of the table
dataTable.drop(dataTable.index[:1],inplace=True)

print(type(dataTable))

coal_row=dataTable.iloc[0][1:26]


print(dataTable)
print(coal_row)

dataTable.loc["test"]=coal_row

print(dataTable)
'''

'''

dataTable.index=dataTable.index+1
dataTable.iloc[0]=coal_row
dataTable=dataTable.sort_index()
print(dataTable)
'''
