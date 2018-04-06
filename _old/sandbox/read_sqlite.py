#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 17:26:23 2018

@author: ernstoldenhof
"""
# General imports
import sqlite3
import os.path
import pandas

# Project imports
from utils import sqlite


# Set paths
BASE_DIR = os.getcwd()
db_path = os.path.join(BASE_DIR, "pricing_db")
excel_path = os.path.join(BASE_DIR, "pricing_data.xlsx")


# Set parameters
table_name_list = ['MAPPING_PAR_REF_AIRPORT', 'MAPPING_PAR_REF_GROUND_HANDLER',
                   'PAR_AIRPORT', 'PAR_GROUND_HANDLER']

def execute_sql(sql, fetch=False):
    with sqlite3.connect(db_path) as db_connection:
        db_cursor = db_connection.cursor()
        db_cursor.execute(sql)
        if fetch:
            return db_cursor.fetchall()



############## SANDBOX ################
            
sql = r"SELECT * FROM MAPPING_PAR_REF_AIRPORT"

vals = _execute_sql(sql, fetch=True)

def insert_into_sqlite(sql, rows):
    with sqlite3.connect(db_path) as db_connection:
        db_cursor = db_connection.cursor()
        db_cursor.execute(sql)
        if fetch:
            return db_cursor.fetchall()




rows = [(999,'drunken kitties'), (2133,'aerer')]
with sqlite3.connect(db_path) as db_connection:
    db_cursor = db_connection.cursor()
    db_cursor.executemany('insert into TABLE artists VALUES (?,?)', rows)
    db_connection.commit()



df = pandas.read_excel('pricing_data.xlsx', sheet_name='PAR_AIRPORT')

with sqlite3.connect(db_path) as db_connection:
    db_cursor = db_connection.cursor()
    db_cursor.executemany('insert into TABLE {} VALUES (?,?)'.format(table_name_list[0]), rows)
    db_connection.commit()

