#!/bin/python
# coding: utf-8
import requests
import json
import psycopg2
import os


#API variables
api_username=os.environ['API_USERNAME']
api_password=os.environ['API_PASSWORD']
api_url=os.environ['API_URL']
#Database credentials
db_username=os.environ['DB_USERNAME']
db_password=os.environ['DB_PASSWORD']
db_database_api=os.environ['DB_API']


conn = psycopg2.connect(database=db_databse_api, host='localhost', port=5432, user='roman', password='snickers')
cur = conn.cursor()

#Load the list of course titles(type_id)

cur.execute('SELECT type_id from schedule')

list_holder = cur.fetchall()

type_id_list = []

for x in list_holder:
    type_id_list.append(x[0])
    
url = api_url

for x in type_id_list:
    new_url = url+x
    caty = requests.get(new_url, auth=(username, password))
    course = json.loads(caty.content) 

    #Know this works
    #SQL = "INSERT INTO course_test_on (course_name, course_title, description, abstract, duration, list_price) VALUES (%s, %s, %s, %s, %s, %s);"
    #trying all the columns
    SQL = "INSERT INTO course (course_code, course_title, vendor, description, abstract, duration, list_price, topic, objective, overview, duration_unit, prerequisits, currency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    Data = (course['name'],course['title'],course['vendor'],course['descriptions']['EN']['description'],course['descriptions']['EN']['abstract'],course['duration'],course['listPrice'],course['descriptions']['EN']['topic'],course['descriptions']['EN']['objective'],course['descriptions']['EN']['overview'],course['durationUnit'],course['descriptions']['EN']['prerequisits'],course['currency'])

    try:
        cur.execute(SQL, Data)
        conn.commit()
        print "success" + course['name']
    except:
        conn.rollback()
        print "error" + course['name'] 

print "success"


conn.close()
