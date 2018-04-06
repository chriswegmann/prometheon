#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  4 12:00:58 2017

@author: ernst
"""
import sqlite3 
import os


import collections
from config import key_mappings_dict   

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

def map_keys(db_dict,key_mappings_dict=key_mappings_dict):
    db_dict = {(key_mappings_dict[key_old] if key_old in key_mappings_dict.keys() 
        else key_old):value for (key_old,value) in db_dict.items() } 
    return db_dict

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
    
    print("TableName\tColumns\tRows\tCells")

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
        
        print("%s\t%d\t%d\t%d" % (table, numberOfColumns, numberOfRows, numberOfCells))
        
        totalTables += 1
        totalColumns += numberOfColumns
        totalRows += numberOfRows
        totalCells += numberOfCells

    print( "" )
    print( "Number of Tables:\t%d" % totalTables )
    print( "Total Number of Columns:\t%d" % totalColumns )
    print( "Total Number of Rows:\t%d" % totalRows )
    print( "Total Number of Cells:\t%d" % totalCells )
        
    cursor.close()
    connection.close()  
###############


def get_col_sqlite(database_path,table_name,col_name):
    with sqlite3.connect(database_path) as conn:
        cursor= conn.cursor()
    
        rowsQuery = "SELECT ({0}) FROM ({1})".format(col_name,table_name)
        cursor.execute(rowsQuery) 
        col_data=cursor.fetchall()
        conn.close()
        return col_data
        



def create_sqlite(database_path,table_name_list,keys_list,
                  key_attributes_list):
    '''
    Creates a sqlite database if it does not yet exist 
    (checks with os.path)    
    '''
    db_exists= os.path.isfile(database_path)
    if not db_exists:   
        print('Database {0} not found, creating... '.format(database_path))
        conn = sqlite3.connect(database_path)
        c=conn.cursor()
        
        for table_name,keys,key_attributes in zip(table_name_list,
                                    keys_list,key_attributes_list):
            ###  Define the insertion command 
            # key_attributes: for now, make all strings              
                
                
            key_plus_attributes=["'" + rr[0] +"' " + rr[1] for rr in zip(keys,
                                 key_attributes)]
            
            insert_command= "CREATE TABLE {0} ({1})"
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


def add_table_to_sqlite(database_path,table_name,keys,key_attributes):
    """Not checked yet"""
    conn = sqlite3.connect(database_path)
    query="create table if not exists {0} ({1})"
    key_plus_attributes=["'" + rr[0] +"' " + rr[1] for rr in zip(keys,
                         key_attributes)]
    
    query = query.format(table_name,
                                        ",".join(key_plus_attributes))            
    c=conn.cursor()
    c.execute(query)
    conn.commit()
    conn.close()

    
def drop_table_from_sqlite(database_path,table_name):
    conn = sqlite3.connect(database_path)
    c=conn.cursor()
    query="drop table if exists {0}".format(table_name)
    c.execute(query)
    conn.commit()
    conn.close()





def add_data_to_sqlite(database_path,table_name,keys,data,flatten_dict=False):
    '''
    Creates a connection to a sqlite database, and writes values into 
    a specified table
    
    database_path: path of sqlite database (string)
    table_name: name of table the data will be written into (string)
    keys: the column names (list)
    data: a list of dicts (list)
    TODO's:
    + (done): option to deal with missing keys in data
      So: make query each time with the keys of data[i]
    - check cursor closing's: necessary?
    '''
    # use " with .. as" statement to guarantee closure 
    #First: fetch 
    with sqlite3.connect(database_path) as conn:
        c= conn.cursor()
        size_before_operation=size_sqlite_table(c,table_name)
        query_template = "INSERT OR IGNORE INTO {0} ({1}) VALUES ({2})"
    
        #print(query)
        
        for record in data:
            if flatten_dict:
                try:
                    record=flatten(record)
                except:
                    continue
            # Only keep the items in the list with known keys
            record={key:value for (key, value) in record.items() if key in keys}
            # Convert things that are not string to string, or remove
            record=convert_values_to_strings(record)
    
            # Put keys within parentheses to deal with special chars like <:>
            encapsulated_keys = ["'" + key + "' " for key in record.keys()]
            query = query_template.format(table_name, ",".join(encapsulated_keys),
                                 ",".join("?" * len(encapsulated_keys)))
            values = list(record.values())
            c = conn.cursor()
    
            c.execute(query, values)
            c.close()
        c=conn.cursor()
        size_after_operation=size_sqlite_table(c,table_name)
        added_rows=size_after_operation['nrow']-size_before_operation['nrow']
        print("Added {0} rows (of {1} suggestions) to table {2}".format(
                added_rows,len(data),table_name))

        conn.commit()
        conn.close()


def size_sqlite_table(cursor,table_name):
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
    return({'nrow':numberOfRows,'ncol':numberOfColumns})



