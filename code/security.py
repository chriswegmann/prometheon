#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 16:12:27 2018

@author: ernst
"""
from werkzeug.security import safe_str_cmp #string comparison (dealing with unicode etc.)
from models.user import UserModel


def authenticate(username, password):
    user = UserModel.find_by_username(username)#.get: can specify default value
    if user and safe_str_cmp(user.password, password):
        return user

def identity(payload):
    user_id = payload['identity']
    return UserModel.find_by_id(user_id)
