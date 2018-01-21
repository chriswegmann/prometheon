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


RAN SUCCESSFULLY (on Linux) 20:00, 07 Jan 2018

TODO's
- For API purpose, this script needs to be called with arguments. sys.arg needs
  to be implemented
- Some tests need to be defined
- ....
"""
######################################################
### Imports         ##################################
######################################################

## General imports 
import sqlite3
import os.path
import pandas as pd
from datetime import datetime
from collections import namedtuple

## Project imports:
# Import sql helpers from utils/
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



required_keys = ['amount', 'ground_handlers', 
                 'airports', 'timestamp']

# The tables and name of the column for the query corresponding to a key in the input
# The collection of names is a namedtuple, goes into a dict

# Define namedtuple to consistently store the sql names for the parameters
TableNames = namedtuple('TableNames','table_a table_b column_name')

# Define the dict with the namedtuple TableNames as value
sql_parameter_tablename_dict = {}
sql_parameter_tablename_dict['airports'] = TableNames('MAPPING_PAR_REF_AIRPORT', 
                            'PAR_AIRPORT', 
                            'AIRPORT_ID')

sql_parameter_tablename_dict['ground_handlers'] = TableNames('MAPPING_PAR_REF_GROUND_HANDLER', 
                            'PAR_GROUND_HANDLER', 
                            'GROUND_HANDLER_ID')

# Return a pandas DataFrame as overview (perhaps, other data structure is more suited?)

base_rate = 1.E-3 # This probably should be in the database in its own table, with FROM, TO
                    
######################################################
### Input           ##################################
######################################################

# quote_request : input to the code
# During development, give a dict here. In production, this will 
# go through sys.args and in arguments in linux

quote_request = {
'amount' : 360E3,
'ground_handlers' : [1, 5, 10],
'airports' : [650, 652, 668],
'timestamp' : (2018, 1, 7)} #Y, M, D

######################################################
### Main Code       ##################################
######################################################
timestamp = datetime(*quote_request['timestamp'])

# Needed functionality:
# - Check that all necessary keys are given (locally)
# - Check that 


# NB: airports/handlers are identified by their parameter_id (parameter). 
# 1. Assert that all required keys are provided

for key in required_keys:
    assert key in quote_request.keys(), 'Aborting: key "{}" missing in request'.format(key)
    
# 2. Get the associated parameters, and fill up the dataframe

def create_quote(quote_request):
    """
    Looks into the database to search for the parameters, and appends the
    results to a DataFrame.
    Returns the quote amount, and the DataFrame with detailed info
    """
    quote_reply = pd.DataFrame(columns=('date', 'amount', 'type', 'parameter_id', 'name', 'factor'))

    # Loop through all items, to search for the "parameters"
    for key, parameter_ids in quote_request.items():
        
        # Test if this is a key that requires a parameter lookup
        if key in sql_parameter_tablename_dict.keys():
                assert type(parameter_ids) == list, ('The values to determine the factors '
                           'must be given as a list')
            
                # Create the sql string
                # Use .format() together with the ._asdict() method of the 
                # named tuple to "pre-format" the string
                # Use ?'s as placeholders (NB: for other sql servers, 
                # the placeholder syntax will be somewhat different)
                sql = ('SELECT B.PARAMETER_VALUE FROM {table_a} AS A '
                       'JOIN {table_b} AS B ON A.PARAMETER_ID = B.PARAMETER_ID '
                       'WHERE A.{column_name} = ? AND '
                       'B.VALID_FROM <= ? AND B.VALID_TO > ?').format(
                **sql_parameter_tablename_dict[key]._asdict())
                
                # Make database connection
                db_connection = sqlite3.connect(db_path)
                
                # Create database cursor
                cursor = db_connection.cursor()
                
                try:
                    
                    for parameter_id in parameter_ids:
                        vals = (parameter_id, timestamp, timestamp)
                        cursor.execute(sql, vals)
                        #print(sql)
                        #print(vals)
                        
                        # fetch the result
                        factor = cursor.fetchone()[0]
                        
                        # Append to the dataframe, by using a dictionary
                        quote_reply = quote_reply.append({'date':timestamp,
                                            'amount':quote_request['amount'],
                                            'type':key,
                                            'parameter_id':parameter_id,
                                            'factor':factor },
                                            ignore_index=True) #NB: we have no name
                except Exception as e:
                    raise e
                finally:
                    db_connection.close()
                    
    # Simplified premium formulat
    premium = quote_request['amount'] * base_rate * quote_reply.product(axis=0)['factor']
    return(premium, quote_reply)
                
        
premium, quote_reply = create_quote(quote_request)
print('premium calculated: {}'.format(premium))
print(quote_reply)     

 
######################################################
### Tests           ##################################
####################################################### 
# Define some tests here:
def do_tests():
    pass

    

do_tests()
