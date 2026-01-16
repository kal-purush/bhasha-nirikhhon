
import requests
import pandas as pd
import json

def mfl_api():
    auth_credentials = {
    'grant_type': 'password',
    'username': 'test@testmail.com',
    'password': 'Test@1234'
    }

    # get access token
    response = requests.post(
        'http://jtqx5pdrbar2uhdcw3kyiwjk6usnwog59b1chcye:87lcWJNWkA5aQC0zNPfZuGnUH36zRMXK3UXy70ckfgE1K9S0yfJ2fU1RcAbroCnYhuUZF7Kjg6PEKy0zb4DwMXCIBYasw8pnxb1sU9Xbcb5NnkoX2LohxWSHBvosnGtZ@api.kmhfltest.health.go.ke/o/token/', data=auth_credentials)
    access_response=json.dumps(response)
    access_token = access_response.get('access_token')
    
    facilities_urls = 'https://api.kmhfltest.health.go.ke/api/facilities/facilities/?format=json&page_size=150'
    headers = {
        'Athorization' : 'Bearer {access_token}',
        'Accept': 'application/json'
        }
    # get all facilities
    facilities_data = requests.post(facilities_urls, headers=headers)


    # pip install -r requeirements.txt --- pandas==1.1.1 
    # requests ==2.0
    # psycopg2==1.2.4
    # read facilities api data as a csv
    # database name is test
    # Create table name as kmhfl_facilities 
    facilities = pd.read_json(facilities_data)
    facilities.to_csv('/home/facilities.csv', index = None)
    
    csv_data = pd.read_csv('/home.facilities.csv')
    # database connection
    try:
        # initiate db connection
        conn = psycopg2.connect(
            host="localhost",
            database="test",
            user="postgres",
            password="postgres")
        
        cur = conn.cursor()
        cur = conn.cursor()
        table_raw = []
        
        #loop through csv data to get individuals columns
        for row in csv_data.results:
        # append individula fields to a python list
            table_raw.append(row.county)
            table_raw.append(row.constituency_name)
            table_raw.append(row.ward_name)
            table_raw.append(row.code)
            table_raw.append(row.created)
            table_raw.append(row.number_of_beds)
            
            # insert row by row to the table
            
            insert_query =  " INSERT INTO kmhfl_facilities  VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
            cur.executemany (insert_query, table_raw)
            conn.commit()
            print("####### record Updated Successfully ###########")
            
        cur.close()
        
        

        
    except (Exception, psycopg2.DatabaseError) as error:
        # print the error from the db
        print(error)
   
# call our fuction to run it   
mfl_api()