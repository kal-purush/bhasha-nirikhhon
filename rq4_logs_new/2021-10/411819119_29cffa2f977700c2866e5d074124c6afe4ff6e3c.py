import json
import re
from math import cos as c
      
# in case if facing issues with pip: curl https://bootstrap.pypa.io/get-pip.py | python3
import mysql.connector  # pip3 install mysql-connector-python
import requests  
      
import time
import sys
try:
        r = requests.post('http://graph.facebook.com/')
        print("Returning a kind of token as a result of a Facebook API test call " + r.json()['error']['fbtrace_id'])
except:
        print(
            "well ok something happened with your internet connection ")
      
try:      
        db = mysql.connector.connect(
            host="database-2.ccwhlpmaeyte.eu-central-1.rds.amazonaws.com",
            port="3306",
            user="admin",
            password="15doramu15",
        )
        mycursor = db.cursor()
        sqlBuff =  "use Disney;"
        mycursor.execute(sqlBuff)
        db.commit()

        mycursor = db.cursor()
        sqlBuf =  "UPDATE Disney SET name = 'Alladin' where id = 7"
        mycursor.execute(sqlBuf)
        db.commit()

        db.close() 
except:
       print("Something went wrong")