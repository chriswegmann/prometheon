#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 17:26:23 2018

@author: ernstoldenhof
"""

import sqlite3
import os.path
import pandas


BASE_DIR = os.getcwd()
db_path = os.path.join(BASE_DIR, "pricing_db")



def execute_sql(sql, fetch=False):
    with sqlite3.connect(db_path) as db_connection:
        db_cursor = db_connection.cursor()
        db_cursor.execute(sql)
        if fetch:
            return db_cursor.fetchall()


sql = r"SELECT * FROM MAPPING_PAR_REF_AIRPORT"

vals = execute_sql(sql, fetch=True)



#rows = [(999,'drunken kitties'), (2133,'aerer')]
#with sqlite3.connect(db_path) as db_connection:
#    db_cursor = db_connection.cursor()
#    db_cursor.executemany('insert into TABLE artists VALUES (?,?)', rows)
#    db_connection.commit()


df = pandas.read_excel('pricing_data.xlsx', sheet_name='PAR_AIRPORT')
