import csv
import psycopg2
import sys
import os
'''
ENDPOINT="nieloa.c7jvzdz9a72c.us-east-1.rds.amazonaws.com"
PORT="5432"
USR="master"
REGION="us-east-1f"
DBNAME="myDatabase"
token = 'masterguy'
'''
ENDPOINT = 'localhost'
PORT ="5432"
USER = "postgres"
DBNAME = 'postgres'
token = 'masterguy'
conn = None
try:
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=token)
    conn.autocommit= True
    cur = conn.cursor()

except Exception as e:
    print("Connected failed due to {}".format(e))   
                 
print(conn)
#Use this for inserting or making changes to values
def dbInsert(query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        print("TRUE")
    except Exception as e:
        print("QUERY error")
#Use this for querying results
def executeQuery(query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        return cursor.fetchall()
    except Exception as e:
        print("QUERY error",e)

