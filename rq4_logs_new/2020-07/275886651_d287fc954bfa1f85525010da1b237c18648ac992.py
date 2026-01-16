''' A handler for User operations in our server '''
# External Modules
from flask import Flask, jsonify, Blueprint, request
from datetime import datetime, date
import urllib.parse
import html
# Internal Modules
import SchoolSystem.data.mongo as db
from SchoolSystem.users.model import User
from SchoolSystem.data.logger import get_logger
from os import path

# def get_user_type():
#     '''Returns the type of user that is logging in'''
#     user = db.login(username, password)
#     return user.type

_log = get_logger(__name__)