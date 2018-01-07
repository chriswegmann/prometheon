#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 14:15 2018
@author: ernstoldenhof

Calculate the pricing based on factors stored in sql database. 
Input is a dictionary giving the details on the contract

{
'amount' : amount to be insured,
'ground_handlers' : list of ground handlers,
'airports' : list of airports,
'timestamp' : the date and time of the contractual agreement
}


RAN ....
  
"""
######################################################
### Imports         ##################################
######################################################

## General imports 
import sqlite3
import os.path
import pandas as pd
from datetime import datetime

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
# During development, give a dict here. In production, this will 
# go through sys.args

# NB: airports/handlers are identified by their parameter_id (parameter). 
 
quote_request = {
'amount' : 360E3,
'ground_handlers' : [1,5, 10],
'airports' : [650, 655,660],
'timestamp' : datetime.now()       }
          
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
            


