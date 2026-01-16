# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 10:47:18 2020
simple two file append
  part 1 of 2 monthly updates >>> csv version
  *** make sure update file has MSA removed and no extra fields at the end of the data (NaN)
  - 
  
@author: AndyOlson
"""

from google.cloud import bigquery
import os
import pandas as pd
    
#Google Bigquery authentication and client initiation
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/mvonoven/Projects/GCP/jacobsmarketing-a0a20419ac82.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/andyolson/Documents/python/jacobsmarketing-0d9cb1eb005c.json"
client = bigquery.Client(project = 'jacobsmarketing')

localfilename = r'N:\Data\GeneralInfo\labor\reBoot\upload\ssamatab1_upload_test.csv'  #   was ssamatab1_upload.csv
localfilename2= r'N:\Data\GeneralInfo\labor\reBoot\upload\ssamatab1_Mth_update.csv'  #dataset_id = 'locations'  # 'locations'

df1 = pd.read_csv(localfilename, thousands=',')  # convert comma objects to INT
df1 = df1.astype({"ZipCode": str,"State FIPS Code": str,"Area FIPS Code - CBSA": str, "CrossFIBS": str, "Unemployment Rate": float})
df1['CrossFIBS'] = df1['CrossFIBS'].apply(lambda x: x.zfill(5)) #zero fill on front
print(df1[['CrossFIBS','ZipCode']])

df2 = pd.read_csv(localfilename2, thousands=',')
df2 = df2.drop(df2[(df2.State == " PR")].index)  # removing NAN & (n) values ~ not working yet
df2 = df2.astype({"ZipCode": str,"State FIPS Code": str,"Area FIPS Code - CBSA": str, "CrossFIBS": str, "Unemployment Rate": float, "Employment": float, "Unemployment": int,})
print (df2.dtypes)



print (df2.head)
df2['CrossFIBS'] = df2['CrossFIBS'].apply(lambda x: x.zfill(5)) #zero fill on front
print(df2[['CrossFIBS','ZipCode']])
#check
#print (df2.dtypes)
#print (df1.dtypes)
#  df2.drop(df1[(df1.State == "PR"
# remove restated month -- testing
# https://stackoverflow.com/questions/29017525/deleting-rows-based-on-multiple-conditions-python-pandas
# https://stackoverflow.com/questions/52456874/drop-rows-on-multiple-conditions-in-pandas-dataframe

# big deal below
new_df1 = df1.drop(df1[(df1.Year == 2020) & (df1.Month == 3)].index)

print (new_df1.head)

new_df1 =new_df1.append(df2, ignore_index = True)
print("---we good?---")
#new_df1 = new_df1.astype({"State FIPS Code": int, "Unemployment Rate": float})  # not working yet
#for col in ["State FIPS Code","Employment","Unemployment"]:
#  new_df1 = new_df1.astype({col: int, "Unemployment Rate": float})

print (new_df1.head)
#new_df1 = new_df1.astype({"ZipCode": str,"CrossFIBS": str})
print(df2)
#add output
path_out = r'N:\Data\GeneralInfo\labor\reBoot'
#new_df1.to_csv(path_out + '\ssama_master.csv', index=False)
print("complete")