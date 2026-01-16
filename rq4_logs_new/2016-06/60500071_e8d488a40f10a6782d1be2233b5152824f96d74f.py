#!/usr/bin/python
# coding: utf-8
#
# filename updSqlToFm.py
# version 0.35 - June 8, 2016 by <Jaap at kroesschell-ictconsulting.ch>
# Objective read sql data and check for changes in the last 24 hours (timestamp)
# 
#
# python 2.7 
#
import unicodedata
import os
import MySQLdb
import datetime
from PyFileMaker import FMServer

# Get access details from env / Or set locally (for debugging only) 
FMDBHOST = os.environ.get('FMDBHOST')	# FMDBHOST = 'host name or ip address'
FMDB 	= os.environ.get('FMDB')	# FMDB = 'fm database name'
FMDBUSER = os.environ.get('FMDBUSER')	# FMDBUSER = 'fm user name'
FMDBPASSWORD = os.environ.get('FMDBPASSWORD')	# FMDBPASSWORD = 'fm password'

SQLDBHOST = os.environ.get('SQLDBHOST')	# SQLDBHOST = 'host name or ip address'
SQLDB = os.environ.get('SQLDB')	# SQLDB = 'sql database name'
SQLDBUSER = os.environ.get('SQLDBUSER')	# SQLDBUSER = 'sql user name'
SQLDBPASSWORD = os.environ.get('SQLDBPASSWORD')	# SQLDBPASSWORD = 'sql password'

FMACCESS = FMDBUSER+':'+FMDBPASSWORD+'@'+FMDBHOST	# Facilitate fm db connection
fm = FMServer('http://'+FMACCESS)	# Facilitate fm db connection
# fm._debug = True	# Enable fms debugging (exposes urls)
fm.setDb(FMDB)	# Declare fms db to be used

# read timestamp for all records, and see if it is changed since moment x and print customer_id

def readSql():
	db = MySQLdb.connect(SQLDBHOST,SQLDBUSER,SQLDBPASSWORD,SQLDB,charset='utf8',
                     use_unicode=True) # Open sql connection 
	cursor = db.cursor()	# Prepare a cursor object using cursor() method			

	cursor.execute("SELECT * FROM customer where update_datetime >'2016-06-08 09:58:01'") 
	print("fetchall:")
	result = cursor.fetchall() 
	for r in result:
	    # print(r)
	    for item in r:
	    	print item, ',',
	# cursor.execute("SELECT * FROM customer where update_datetime >'2016-06-08 09:58:01'") 
	# print("\nfetch one:")
	# res = cursor.fetchone() 
	# print(res)

	return

def run():
	readSql()
	return

run()

