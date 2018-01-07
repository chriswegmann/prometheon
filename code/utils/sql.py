#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 12:00:58 2017

@author: Ernst Oldenhof

TODO's
- To generalize code for any SQL type (sqlite, postgres), write functions
  that return a connection for that SQL type, and pass those connections
- ...

"""
import sqlite3 
import os
import pdb


import collections

def flatten(d, parent_key='', sep='_'):
    """
    Code from StackOverflow
    Recursive algorithm that returns a flattened dict
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)



def convert_values_to_strings(dict,list_sep=', '):
    """
    returns a dict with all values (converted to) strings
    if conversion fails, item is removed
    """
    d=dict.copy()
    for key in dict.keys():
        if not isinstance(dict[key],str):
            try:
                d[key]=list_sep.join(dict[key])
            except:
                del d[key]
    return d



####################
#Following two functions (Print, Describe):
## <author>Pieter Muller</author>
## <date>2012-11-14</date>
#Code from 
#"https://pagehalffull.wordpress.com/2012/11/14/
#python-script-to-count-tables-columns-and-rows-in-sqlite-database/"

tablesToIgnore = ["sqlite_sequence"]


        

def describe_db(dbFile):
    connection = sqlite3.connect(dbFile)
    cursor = connection.cursor()
    
    print("TableName\t\t\t\t\tColumns\tRows\tCells")

    totalTables = 0
    totalColumns = 0
    totalRows = 0
    totalCells = 0
    
    # Get List of Tables:      
    tableListQuery = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY Name"
    cursor.execute(tableListQuery)
    tables = map(lambda t: t[0], cursor.fetchall())
    
    for table in tables:
    
        if (table in tablesToIgnore):
            continue            
            
        columnsQuery = "PRAGMA table_info(%s)" % table
        cursor.execute(columnsQuery)
        numberOfColumns = len(cursor.fetchall())
        
        rowsQuery = "SELECT Count() FROM %s" % table
        cursor.execute(rowsQuery)
        numberOfRows = cursor.fetchone()[0]
        
        numberOfCells = numberOfColumns*numberOfRows
        
        print("%s\t\t\t\t\t%d\t%d\t%d" % (table, numberOfColumns, numberOfRows, numberOfCells))
        
        totalTables += 1
        totalColumns += numberOfColumns
        totalRows += numberOfRows
        totalCells += numberOfCells

    print( "" )
    print( "Number of Tables:\t\t%d" % totalTables )
    print( "Total Number of Columns:\t\t%d" % totalColumns )
    print( "Total Number of Rows:\t\t%d" % totalRows )
    print( "Total Number of Cells:\t\t%d" % totalCells )
        
    cursor.close()
    connection.close()  
###############


def get_col_sqlite(database_path, table_name, col_name):
    with sqlite3.connect(database_path) as conn:
        cursor= conn.cursor()
    
        rowsQuery = "SELECT ({0}) FROM ({1})".format(col_name, table_name)
        cursor.execute(rowsQuery) 
        col_data = cursor.fetchall()
        conn.close()
        return col_data
        



def create_sqlite(database_path, table_name_list, keys_list,
                  key_attributes_list):
    '''
    Creates a sqlite database if it does not yet exist 
    (checks with os.path)    
    '''
    db_exists = os.path.isfile(database_path)
    if not db_exists:   
        print('Database {0} not found, creating... '.format(database_path))
        conn = sqlite3.connect(database_path)
        c = conn.cursor()
        
        for table_name, keys, key_attributes in zip(table_name_list,
                                    keys_list, key_attributes_list):
            ###  Define the insertion command 
            # key_attributes: for now, make all strings              
                
                
            key_plus_attributes=["'" + rr[0] +"' " + rr[1] for rr in zip(keys,
                                 key_attributes)]
            
            insert_command = "CREATE TABLE {0} ({1})"
            insert_command = insert_command.format(table_name,
                                                ",".join(key_plus_attributes))            
            c.execute(insert_command)
        conn.commit()
        conn.close()
        
        print('Database {0} created, with tables {1}.'.format(
                database_path,",".join(table_name_list)))
    else:
        print('Database {0} found.'.format(
                database_path))


def add_table_to_sqlite(database_path, table_name, keys, key_attributes):
    """Not checked yet"""
    conn = sqlite3.connect(database_path)
    query = "create table if not exists {0} ({1})"
    key_plus_attributes = ["'" + rr[0] +"' " + rr[1] for rr in zip(keys,
                         key_attributes)]
    
    query = query.format(table_name, ",".join(key_plus_attributes))            
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    conn.close()

    
def drop_table_from_sqlite(database_path, table_name):
    conn = sqlite3.connect(database_path)
    c = conn.cursor()
    query = "drop table if exists {0}".format(table_name)
    c.execute(query)
    conn.commit()
    conn.close()


def add_data_to_sql(connection, table_name, data, update=False, verbose=0):
    '''
    Upgrade of "add_data_to_sqlite": expects a db connection object
    Creates a connection to a sqlite database, and writes values into 
    a specified table
    
    database_path: path of sqlite database (string)
    table_name: name of table the data will be written into (string)
    data: a list of dicts (list). Each dict represents a row, with 
          {columname1:value1, columname2:value2, ... }
          
    NB: problems with "Database is locked" can occur when the db is simultaneously
    open in DBVisualizer. 

    '''
    # Open the cursor for db transactions 
    # pdb.set_trace()
    cursor = connection.cursor()
    
    # Define the sql template
    if update: 
        query_template = "INSERT OR UPDATE INTO {0} ({1}) VALUES ({2})"
    else:
        query_template = "INSERT OR IGNORE INTO {0} ({1}) VALUES ({2})"
    
    # try/except/finally to guarantee database closure if something goes wrong    
    try:
        if verbose > 0:
            # determine size of table before operation
            size_before_operation = size_sqlite_table(cursor, table_name)
        
        for record in data:
    
            # Put keys within parentheses to deal with special chars like <:>
            encapsulated_keys = ["'" + key + "' " for key in record.keys()]
            query = query_template.format(table_name, ",".join(encapsulated_keys),
                                 ",".join("?" * len(encapsulated_keys)))
            values = list(record.values())
            
            # Print the SQL statement and the values if desired
            if verbose >1:
                print('###############')
                print(query)
                print(values)
                print('###############')
                      
            # Execute the cursor
            cursor.execute(query, values)
            connection.commit()
            
        # Print how many rows were added if desired    
        if verbose > 0:
            # in try/except such that the insert operation does not fail upon
            # failing to get size change info
            try:
                size_after_operation = size_sqlite_table(cursor, table_name)
                added_rows = size_after_operation['nrow'] - size_before_operation['nrow']
                print("Added {0} rows (of {1} suggestions) to table {2}".format(
                    added_rows, len(data), table_name))
            except:
                print('Could not retrieve number of additions')
    
        
    except Exception as e:
        raise e
        
    finally:
        # No matter what happens, close the connection
        connection.close()


def add_data_to_sqlite(database_path, table_name, data, flatten_dict=False):
    '''
    Creates a connection to a sqlite database, and writes values into 
    a specified table
    
    database_path: path of sqlite database (string)
    table_name: name of table the data will be written into (string)
    data: a list of dicts (list). Each dict represents a row, with 
          {columname1:value1, columname2:value2, ... }
    TODO's:
    + (done): option to deal with missing keys in data
      So: make query each time with the keys of data[i]
    - check cursor closing's: necessary?
    '''
    # use " with .. as" statement to guarantee closure of connection    
    with sqlite3.connect(database_path) as conn:
        c= conn.cursor()
        
        # get the 
        size_before_operation=size_sqlite_table(c,table_name)
        query_template = "INSERT OR IGNORE INTO {0} ({1}) VALUES ({2})"
    
        #print(query)
        
        for record in data:
            if flatten_dict:
                try:
                    record = flatten(record)
                except:
                    continue

            # Convert things that are not string to string, or remove
            #pdb.set_trace()
            #record = convert_values_to_strings(record)
    
            # Put keys within parentheses to deal with special chars like <:>
            encapsulated_keys = ["'" + key + "' " for key in record.keys()]
            query = query_template.format(table_name, ",".join(encapsulated_keys),
                                 ",".join("?" * len(encapsulated_keys)))
            values = list(record.values())
            print('###############')
            print(query)
            print(values)
            print('###############')
            c.execute(query, values)
        size_after_operation = size_sqlite_table(c, table_name)
        added_rows = size_after_operation['nrow'] - size_before_operation['nrow']
        print("Added {0} rows (of {1} suggestions) to table {2}".format(
                added_rows, len(data), table_name))

        conn.commit()
        try:
            conn.close()
        except:
            print('could not close database connection')


def size_sqlite_table(cursor, table_name):
    """ 
    Returns a dict with keys "nrow" and "ncol"
    input: a cursor object, and table name
    """
    #Inspired by code of Pieter Muller
    columnsQuery = "PRAGMA table_info({0})".format(table_name)
    cursor.execute(columnsQuery)
    numberOfColumns = len(cursor.fetchall())
    
    rowsQuery = "SELECT Count() FROM ({0})".format(table_name)
    cursor.execute(rowsQuery)
    numberOfRows = cursor.fetchone()[0]
    return({'nrow':numberOfRows, 'ncol':numberOfColumns})



