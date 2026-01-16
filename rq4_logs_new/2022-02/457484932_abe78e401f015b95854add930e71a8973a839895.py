# Author : Ria Thomas
# Description : The script is to get the British Columbia Covid cases data for the Tableau Dashboard. Also to clean and transform it as required.
# Source : https://resources-covid19canada.hub.arcgis.com/pages/open-data

import pandas as pd
import requests
import json
import datetime
import sqlalchemy as db
import keyring
import pymysql

def load_table():
    #engine_dest = db.create_engine('mysql+pymysql://root:'+keyring.get_password('mysql_db', 'root')+'@localhost/db_dash?host=localhost?port=3306')
    #table_name = 'bc_covid_data'
    #query = 'SELECT * from db_dash.bc_covid_data'
    #data_db = pd.read_sql(query, engine_dest)
    print('Getting data')
    response_api = requests.get("https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/BC_COVID19_Dashboard_Case_Details_Production/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json")
    print('Got the data! Now cleaning..')
    data = response_api.text
    parse_json = json.loads(data)
    df = pd.json_normalize(parse_json['features'])
    df['attributes.Reported_Date'].max()
    df["Reported_Date"] = df.apply(lambda x: datetime.datetime.fromtimestamp(float(x['attributes.Reported_Date']) / 1000).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)

    df.rename(
        columns =
            {'attributes.HA' : 'HA',
             'attributes.Sex' : 'Sex',
             'attributes.Age_Group' : 'Age_Group',
             'attributes.ObjectId' : 'ObjectId'},
             inplace = True
    )

    df.drop(columns = ['attributes.Reported_Date'], inplace = True)
    df_final = df[['Reported_Date', 'HA', 'Sex', 'Age_Group', 'ObjectId']]
    df_final.to_csv('./data/B.C._COVID-19_-_Case_Details.csv', index = False)
    #print('All done! Getting ready to load it to the table')
    #conn = engine_dest.connect()
    #conn.execute("TRUNCATE TABLE " + table_name)
    #df.to_sql(table_name, con=engine_dest, if_exists='append', index = False)
    print('Done loading! Ready to be used in the reports.')

def main():
    load_table()

if __name__ == "__main__":
    main()


import os
cwd = os.getcwd()

# Print the current working directory
print("Current working directory: {0}".format(cwd))

import re
s_time = re.sub('\D','','/Date(1585810800000)/')

datetime.datetime.fromtimestamp(float(s_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')