#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 17:26:23 2018
@author: ernstoldenhof

Populate an empty sql database, initialized with tables and column names, 
from an Excel file that has sheet names corresponding to table names and
column headers corresponding to the columns in the database. 


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

#cwd = os.getcwd()
#if not cwd in sys.path:
#    pass
#    sys.path.append(cwd)

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
    
    
# Following code requires the presence of an .sqlite db at location
# db_path, with tables corresponding to the Excel sheet names, and
# column names corresponding to the (first part) of the Excel column headers
          
# Read Excel file with pandas, sheet by sheet      
for sheet_name in table_name_list:
    
    # Open the sqlite connection
    db_connection = sqlite3.connect(db_path) 
    
    # Read the excel files with pandas
    df_tmp = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    # List that will consist of dicts, one dict per row
    table_values_list =[]
    
    # Make for each row a dictionary with column name : value
    for row in df_tmp.iterrows():
        # row[0] contains the index
        # row[1] is a Pandas Series
        # Because the column names are formatted: "COLUMN_NAME<whitespace>TYPE",
        # split on whitespace and get the first entry only 
        
        col_values = {key.split(' ')[0]:datetime_cast(value) for 
                      key,value in row[1].iteritems()}
        
        table_values_list.append(col_values)
        # NB: col_values is a dictionary with column_name: value
        
    # Add all rows to the table with sheet_name
    add_data_to_sql(db_connection, table_name=sheet_name, 
                    data=table_values_list,
                    update=False)

                
            
            
       
    

############## SANDBOX ################
            


