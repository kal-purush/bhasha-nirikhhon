from __future__ import print_function
import httplib2
import os
import json

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from secret import py_cls_con_str_wmapp as con
from secret import py_cls_mysql_query as wm_sql

import MySQLdb

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

CON_CLS = con.cls_connect_string()
SQL_CLS = wm_sql.cls_mysql_query()
SCOPES = 'https://www.googleapis.com/auth/analytics.readonly'
APPLICATION_NAME = CON_CLS.constr_ga_application_name()
CLIENT_SECRET_FILE = CON_CLS.constr_ga_client_secret()
PROFILE_ID = CON_CLS.constr_ga_profile_id()
CREDENTIAL_FILE = CON_CLS.constr_ga_credential_file()

def get_credentials():
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,CREDENTIAL_FILE)

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_results(service, profile_id):
    return service.data().ga().get(
        ids='ga:' + profile_id,
        start_date='7daysAgo',
        end_date='today',
        dimensions='ga:date,ga:pagePath',
        metrics='ga:sessions,ga:pageviews',
        max_results=10000).execute()


def con_wmapp():
    con_dict = CON_CLS.constr_wmapp()
    
    connection = MySQLdb.connect(host=con_dict["con_host"], 
                                 port=con_dict["con_port"], 
                                 user=con_dict["con_user"], 
                                 passwd=con_dict["con_passwd"], 
                                 db=con_dict["con_db"], 
                                 charset=con_dict["con_charset"]
                                )
    return connection

def sql_exe_func(insert_data):
    connection = con_wmapp()

    try:
        str_sql = SQL_CLS.sql_tbl_ga_daily_pagepath_insert()
        cursor = connection.cursor()
        
        for i in range(len(insert_data)):
            cursor.execute(str_sql, (insert_data[i]))

        connection.commit()

    except MySQLdb.Error as e:
        print('MySQLdb.Error: ', e)

    finally:
        connection.close()        


def main():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('analytics', 'v3', http=http)
    profile = PROFILE_ID
    results = get_results(service, profile).get('rows')
    sql_exe_func(results)

if __name__ == '__main__':
    main()