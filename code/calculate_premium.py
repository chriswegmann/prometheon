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
### Input           ##################################
######################################################
# During development, give a dict here. In production, this will 
# go through sys.args

# NB: airports/handlers are identified by their parameter_id (parameter). 
 
quote_request = {
'amount' : 360E3,
'ground_handlers' : [1,5, 10],
'airports' : [650, 655,660],
'timestamp' : datetime.now()       }

######################################################
### Main Code       ##################################
######################################################


sql = """SELECT B.PARAMETER_VALUE FROM {table_a} AS A 
JOIN {table_b} AS B ON A.PARAMETER_ID = B.PARAMETER_ID WHERE A.{column_id} = ? AND 
B.VALID_FROM <= ? AND B.VALID_TO > ? """.format(table_a='MAPPING_PAR_REF_AIRPORT',
                table_b='PAR_AIRPORT', column_id='AIRPORT_ID')

   
                
            
            
       
    

############## SANDBOX ################
       
# Read Excel file with pandas, sheet by sheet      
    
    # Open the sqlite connection
db_connection = sqlite3.connect(db_path) 
cursor = db_connection.cursor()
cursor.execute(sql, (600, datetime.now(), datetime.now() ))          
cursor.fetchall()

