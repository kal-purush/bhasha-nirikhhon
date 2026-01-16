import pandas as pd
import numpy as np
import sys
import glob
import datetime
import requests
import datetime as dt
import re
import simplekml

from pandas import ExcelWriter
from pandas import ExcelFile

#setup all variables for file handling
interesting_cols=[0,3,4,6,14,15,29,30,49,51,52,54,56,57]     #specify columns to import into dataframes
datepattern='\d{2}-[a-zA-Z]{3}-\d{2}'
redate=re.compile(datepattern)
parser = lambda date: dt.datetime.strptime(date, '%d-%b-%y') if redate.match(date) else ''
google_maps_api_key = 'AIzaSyBiWvlB5pjXhgaKnTFMUhK5hX4MaCtBsIg'
timestamp=dt.datetime.now().strftime("%Y-%m-%d-%H%M%S")#capture current date and local time represented as string

df_savm = pd.read_csv('c:\\cce\\savm_source\\savm.csv',sep=',', engine='python', usecols=interesting_cols, parse_dates=[3], date_parser=parser)

df_savm.columns=['SAVID','Company_Name','AM','Created Date','AM_ID','AM_Team','Vertical','Sub_Vertical','Address','City','ZIP','State','GUID','GU_Name']


df_savm.drop_duplicates({'SAVID'}, keep='first',inplace=True)
df_savm['AM_ID']= np.where(df_savm['AM_Team'].isnull(), df_savm['AM_ID'], df_savm['AM_Team'])
df_savm.drop('AM_Team', axis=1, inplace=True)
#print(df_savm.AM.count())
df_savm['Country']='US'
df_savm['AM_ID'].replace(to_replace='[^a-zA-Z0-9]+',value='-',inplace=True,regex=True)
print(df_savm.AM.value_counts())
print(df_savm[df_savm.Vertical=='Health Care'].AM.value_counts())
g_am=df_savm.groupby('AM_ID')

for am, df in g_am:
    df.to_csv("c:/cce/Output/SAVM_"+am+'-'+timestamp+".csv", index=False)  #write  dataframe to csv file

