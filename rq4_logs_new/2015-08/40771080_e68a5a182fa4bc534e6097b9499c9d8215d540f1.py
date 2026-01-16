import MySQLdb as mdb
import json
import sys
import os
from datetime import datetime as dt

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'tc_configuration'))
# globals
from conn import *

def getConnection():
	return mdb.connect(server, user, passwd, db)


def add_user(userDict):
	#get the connection cursor
	conn = getConnection() #holding onto connection so we can commit the additions later
	cursor = conn.cursor()
	#Should first check if the user exists (has a record with tcid in webappusers)
	#Then, if they don't have a record, try to match on patient id, they must have a record in the Patient table to create an account.
	#May need to log various things here and give feedback to the user based on what happens.
	try:
		# first check if the email already exists in the TCDB
		cursor.execute("select email from webappusers where email=%s", [userDict['email']])
		exists = cursor.fetchall()
		if len(exists): # already registered or put in a non-unique email address
			return False
		# then check if the tcid exists
		cursor.execute("select dob from webappusers where tcid=%s", [userDict['tcid']])
		exists = cursor.fetchall()
		# if the tcid does not exist, they got it wrong. If it does exist, confirm the returned dob with the provided dob.
		if len(exists) == 0 or exists[0][0] != userDict['dob']:
			return False
		#put the values into an array that will be passed into the prepared statement		
		values = [userDict['first_name'], userDict['last_name'], userDict['password'], userDict['email'], userDict['dob'], userDict['tcid']]
		cursor.execute("update webappusers set first_name=%s, last_name=%s, hash=%s, email=%s, dob=%s where tcid=%s", values)
	except Exception as e:
		#cursor.close()
		return False
	#if no exceptions, commit the addition
	conn.commit()
	#cursor.close()
	return True
	
def get_user_hash(email):
	#get the connection cursor
	cursor = getConnection().cursor()
	#get the password hash form the database *Note: email should be scrubbed by now because this function is internal*
	cursor.execute("select hash from webappusers where email = %s", [email])
	h = cursor.fetchone()
	if len(h) > 0:
		return h[0]
	return None

def get_user_with_email(email):
	#get the connection cursor
	cursor = getConnection().cursor()
	try:
		cursor.execute("select tcid, first_name, last_name, email, dob from webappusers where email = %s", [email])
		desc = []
		for d in cursor.description: #get a list of the column names
			desc.append(d[0])
		result = cursor.fetchall()
		if len(result) > 1:
			return None
			
		return dict(zip(desc, result[0]))
	except:
		return None
	
	return None

def get_user_with_tcid(tcid):
	#get the connection cursor
	cursor = getConnection().cursor()
	try:
		cursor.execute("select tcid, first_name, last_name, email, dob from webappusers where tcid = %s", [tcid])
		desc = []
		for d in cursor.description: #get a list of the column names
			desc.append(d[0])
		result = cursor.fetchall()
		if len(result) > 1:
			return None
		
		return dict(zip(desc, result[0]))

	except Exception as e:
		return None
	
	return None

def store_hra_answers(tcid, hra_answers):
	#get the connection cursor
	conn = getConnection()
	cursor = conn.cursor()
	try:
		valueCount = "%s, "
		columns = []
		values = [tcid]
		for answer in hra_answers:
			valueCount += "%s, "
			columns.append(answer)
			values.append(hra_answers[answer])
		valueCount = valueCount[:-2]
		query = "insert into hra_answers (tcid, %s) values (" % ", ".join(columns)
		query += valueCount + ")"
		cursor.execute(query, values)
	except Exception as e:
		return None
	
	conn.commit()
	return True


def get_hra_results(tcid):
	conn = getConnection()
	cursor = conn.cursor()
	try:
		cursor.execute("select * from hra_answers where tcid = %s", [tcid])
		#build the return dict
		desc = []
		for d in cursor.description: #get a list of the column names
			desc.append(d[0])
		result = cursor.fetchall()
		if len(result) != 1:
			return None
		return dict(zip(desc, result[0]))
	except Exception as e:
		return None
	return None	
			
