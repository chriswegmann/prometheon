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
- Move construction of dicts etc to config.py
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
# Import our data path from our project configuration
from project_config.config import (sql_parameter_tablename_dict,
                                  required_keys,
                                  base_rate,
                                  db_path)


######################################################
### Input           ##################################
######################################################

# quote_request : input to the code
# During development, give a dict here. In production, this will
# go through sys.args and in arguments in linux

quote_request = {
                'amount' : 360E3,
                'ground_handlers' : [1, 5, 10],
                'airports' : [650, 652, 890],
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
    assert key in quote_request.keys(), ('Aborting: key "{}" missing in '
                                        'request'.format(key))

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





######################################################
### Tests           ##################################
#######################################################
# Define some tests here:
def do_tests():
    pass


if __name__ == '__main__':

    do_tests()
    premium, quote_reply = create_quote(quote_request)
    print(premium)
    print(quote_reply)
