#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 17:26:23 2018

@author: ernstoldenhof

RAN SUCCESSFULLY Jan 7th, 13:50. 
- Checked: all tables are populated
- In pricing_data_model.sql: added primary keys and foreign keys to guarantee 
  integrity and avoid duplicates
- NB: simultaneous having the DBVisualizer connection leads to errors with 
  "OperationalError: database is locked". Disconnect in DBVisualizer before running this
  
"""
######################################################
### Imports         ##################################
######################################################

## General imports 
import sqlite3
import os.path
import pandas as pd
import sys

cwd = os.getcwd()
sys.path.append(cwd)

## Project imports:
# Import sql helpers from utils/
from utils.sql import add_data_to_sql, describe_db
# Import our data path from our project configuration
from prom_config.prom_config import DATA_PATH

######################################################
### Set parameters  ##################################
######################################################
## Set patsh 
db_path = os.path.join(DATA_PATH, "pricing_db.db")
excel_path = os.path.join(DATA_PATH, "pricing_data.xlsx")


## Set other parameters
table_name_list = ['MAPPING_PAR_REF_AIRPORT', 'MAPPING_PAR_REF_GROUND_HANDLER',
                   'PAR_AIRPORT', 'PAR_GROUND_HANDLER']


######################################################
### Main Code       ##################################
######################################################
#describe_db(db_path)

# lambda function for type casting to datetime
def datetime_cast(dt):
    if type(dt) == pd._libs.tslib.Timestamp:
        return dt.to_pydatetime()
    else:
        return dt

# Read Excel file with pandas, sheet by sheet

for sheet_name in table_name_list:
    db_connection = sqlite3.connect(db_path) 
    df_tmp = pd.read_excel(excel_path, sheet_name=sheet_name)
    table_values_list =[]
    
    # Make for each row a dictionary with column name : value
    for row in df_tmp.iterrows():
        # row[0] contains the index
        # row[1] is a Pandas Series
        # Because the column names are formatted: "COLUMN_NAME<whitespace>TYPE",
        # split on whitespace and get the first entry only 
        
        col_values = {key.split(' ')[0]:datetime_cast(value) for 
                      key,value in row[1].iteritems()}
        
        # ugly hack to cast from Timedate (pandas format) to timedate.timedate
        # col_values['VALID_FROM'] = col_values['VALID_FROM'].to_pydatetime()
        table_values_list.append(col_values)
        # NB: col_values is a dictionary with column_name: value
    add_data_to_sql(db_connection, sheet_name, table_values_list)

                
            
            
       
    

############## SANDBOX ################
            


