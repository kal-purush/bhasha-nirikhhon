#!/usr/bin/python

# Debug mode on
import cgitb
cgitb.enable()

#Print html headers
print (“Content-Type: text/html\n\n”)

#Connect to the db
import pymysql
con = pymysql.connect(
	  db=’yoga’,
	  user=’root’,
  passwd=’yourpassword’,
  host=’localhost’)
#print contents
try:
    with con.cursor() as cur:
		  cur.execute(“Select * from instructors”)
		  rows = cur.fetchall()
		  for row in rows:
			  print(f’{row[0]} {row[1]}’)

finally:
	con.close()