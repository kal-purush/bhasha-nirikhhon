import missingno as masno
import pandas as pd
import numpy as np
import secretplacing as ccd
import psycopg2
import sqlalchemy as db
import os

from sqlalchemy import create_engine, Column, Integer, String, Sequence, Float,PrimaryKeyConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.sql import *

# load environment variables
DB_USER = ccd.userdb
DB_PASS = ccd.dpass
DB_NAME = ccd.dbs
DB_IP = ccd.dbip
DB_PORT = ccd.dbpt

def connect_db():
    #_load_db_vars()
    # create db create_engine
    dbs = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_IP}:{DB_PORT}/{DB_NAME}')
    connection = dbs.connect()
    metadata = db.MetaData()
    return dbs

Base = declarative_base()

def list_file():
    path = 'dataset/'
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(file))
    return files, path

def list_df():
    path = 'dataset/'
    df_lst = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                df_lst.append(os.path.join(file).split(".")[0])
    return df_lst

#contructing dataset using pd.dataframe from dataset folder (its only read csv)
def construct_ds():
    path = 'dataset/'
    mod = 'models.'
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(file))
    for f in files:
        print(mod+f.split(".")[0])
        exec("%s = pd.read_csv('%s')" % (f.split(".")[0],path+f),globals())

#created initied database, its return value integer to limit the database upload each database to posgresql
def construct_dbs(limitbosku):
    engine = connect_db()
    files,path = list_file()
    df_lst = list_df()
    
    #reading the datasets
    for f in files:
        exec("%s = pd.read_csv('%s')" % (f.split(".")[0],path+f))
        
    #limiting the datasets for uploading to postgres
    #limitbosku = 10
    if (limitbosku == 0):
        limitbosku = 1
        for d in df_lst:
            exec("%s = %s.sample(n=%s, random_state=1, replace=False)" % (d, d, limitbosku))
    elif limitbosku in ['unlimited', 'full', 'all', 'kabeh', 'semua']:
        limitbosku = 5
        for d in df_lst:
            exec("%s = %s.sample(n=%s, random_state=1, replace=False)" % (d, d, limitbosku))
    else:
        limitbosku = limitbosku
        for d in df_lst:
            exec("%s = %s.sample(n=%s, random_state=1, replace=False)" % (d, d, limitbosku))
    
    #Engineer Alchemist GOOOOO!!!
    for alch in df_lst:
        exec("%s.to_sql('%s', engine, if_exists='replace', index=False)" % (alch,alch))
    #SET PK SK_ID_CURR on app_loan, SK_ID_PREV on prev_app, SK_ID_BUREAU on bureau
    engine.execute('ALTER TABLE public.application_train ADD CONSTRAINT application_train_pk PRIMARY KEY ("SK_ID_CURR");')
    engine.execute('ALTER TABLE public.bureau ADD CONSTRAINT bureau_pk PRIMARY KEY ("SK_ID_BUREAU")')
    engine.execute('ALTER TABLE public.previous_application ADD CONSTRAINT previous_application_pk PRIMARY KEY ("SK_ID_PREV")')
    
def fetchByQuery(self, query):
    fetchQuery = self.connection.execute(f"SELECT * FROM {query}")
        
    for data in fetchQuery.fetchall():
        print(data)

#limiting the dataset, oh is refer to dataset, yes is the limitter use number only and dont input yes > len(dataset)
def limit_ds(oh, yes):
    no = 1
    limit = oh.sample(n=yes, random_state=no, replace=False)
    return limit