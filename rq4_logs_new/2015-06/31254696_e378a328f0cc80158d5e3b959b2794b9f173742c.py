: Running this script multiple times creates multiple entries in flarm_db.
# This should be avoided by a db clean up at the end of the script.


import string
import requests
import sqlite3

# Download OGN Device Database content to memory
try:
    OGN_db = "http://ddb.glidernet.org/download/"
    flarmnet_db = "http://www.flarmnet.org/files/data.fln"
    #r = requests.get(flarmnet_db)
    r = requests.get(OGN_db)
    #print r.status_code
    #print r.headers
    #print r.content
    #print r.text
except:
    # This does not catch 502 Bad Gateway error messages :-(
    print "Failed to connect to OGN Device database, exit."
    #print "Failed to connect to flarmnet db, exit"
    exit()
    
# Write OGN content from memory to file
data = r.content    
flm = open("OGN_data", "w")
flm_txt = open("OGN_data_txt", "w")
flm_ln = len(r.content) - 1
print "OGN db length: ", flm_ln
try:
    for i in range(0, flm_ln, 1):
        c = "%c" % data[i]
        flm.write(c)
except :
    print "Error writing OGN_data"   
    exit()
flm.close()

# Code modified to this point
# quit()

#db = open("data.fln", 'r')
db = open("OGN_data", 'r')
# Read first line and convert to number
x = db.readline()
# val = int(x, 16)
text = x.split(',')
print "First line is: ", text

try:
    # Creates or opens a file called mydb with a SQLite3 DB
    dbflarm = sqlite3.connect('flogger.sql3')
    print "Create OGN_db table in flogger.sql3"
    # Get a cursor object
    cursor = dbflarm.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS
                        flarm_db(id INTEGER, flarm_id TEXT PRIMARY KEY, airport STRING, type TEXT, registration TEXT, radio TEXT)''')
    # Commit the changes
#    dbflarm.commit()
# Catch the exception
except Exception as e:
    # Roll back any change if something goes wrong
    print "Failed to create flarm_db"
    dbflarm.rollback()
    raise e

# Start processing file content
#DEVICE_TYPE,DEVICE_ID,AIRCRAFT_MODEL,REGISTRATION,CN,TRACKED,IDENTIFIED

i = 1
line = ""
# nos_lines = val 
while True:
    try:
	line = db.readline()
	line = line.replace("'","")
	text = line.split(',')
	text[6] = text[6].replace("\r","")
        text[6] = text[6].replace("\n","")
        string = ""
	# print text
        # print "read: ", i, " returns: ", text
        
	# Check if tracking and identification is allowd in record.
	if text[5] == "N":
		print text[1],' does not want to be tracked'
	if text[6] == "N":
		print text[1]," does not want to be identified"
	
	# Check if it is a valid message to process
	if (text[0] == "F" or text[0] == "O" or text[0] == "I") and text[5] == "Y" and text[6] == "Y":
		# Process Flarm messages here
		# print 'Flarm/OGN/I message - start processing.'

		ID = text[1]
       		# OGN ddb does not contain Airport info. Left empty.
		Airport = ""
		Type = text[2]
        	Registration = text[3]
        	# OGN ddb does not contain Radio info. Left empty.
		Radio = ""
        	# print "Line: ", i-1, " ID: ", ID,  " Airport: ", Airport, " Type: ", Type, " Registration: ", Registration,  " Radio: ", Radio
       		row = "%s__%s__%s__%s__%s\n" % (ID, Airport, Type, Registration, Radio)
        	flm_txt.write(row)
        	
		# Check if entry is alread present
		try:
			cursor.execute('''SELECT count(flarm_id) from flarm_db where flarm_id = :flarm_id''',{'flarm_id':ID})
			test = cursor.fetchone() 
			# print test
			# print ID
			if test == (1,):
				# print "ID already present - Update."
				cursor.execute('''UPDATE flarm_db SET type = :type, registration = :registration WHERE flarm_id = :flarm_id ''', {'type': Type, 'registration': Registration, 'flarm_id': ID})
				# print 'Update complete'
				continue
			else:
				cursor.execute('''INSERT INTO flarm_db(flarm_id, type, registration)
                           VALUES(:flarm_id, :type, :registration)''',
                            {'flarm_id': ID, 'type': Type, 'registration': Registration})
		except:
			print "Something when wrong while updating flarm_d"

		# try:
            	# 	cursor.execute('''INSERT INTO flarm_db(flarm_id, type, registration)
                #           VALUES(:flarm_id, :type, :registration)''',
                #            {'flarm_id': ID, 'type': Type, 'registration': Registration})
        	# except :
           	# 	print "Flarm_db insert failed for following ID: ", ID
           	# 	dbflarm.commit()
	else:
		# Anything else
		print text
		print 'Anything else - do nothing.'
	
        
	
	i = i + 1
        
    except:
        print "Last line is: ", i
        dbflarm.commit()
        exit()
dbflarm.commit()#(?<_id> .{6})
#(?<owner> .{21})
#(?<airport> .{21})
#(?<type> .{21})
#(?<registration>.{7})
#(?<tail> .{3})
#(?<radio> .{7}) 

# TODO: Running this script multiple times creates multiple entries in flarm_db.
# This should be avoided by a db clean up at the end of the script.


import string
import requests
import sqlite3

# Download OGN Device Database content to memory
try:
    OGN_db = "http://ddb.glidernet.org/download/"
    flarmnet_db = "http://www.flarmnet.org/files/data.fln"
    #r = requests.get(flarmnet_db)
    r = requests.get(OGN_db)
    #print r.status_code
    #print r.headers
    #print r.content
    #print r.text
except:
    # This does not catch 502 Bad Gateway error messages :-(
    print "Failed to connect to OGN Device database, exit."
    #print "Failed to connect to flarmnet db, exit"
    exit()
    
# Write OGN content from memory to file
data = r.content    
flm = open("OGN_data", "w")
flm_txt = open("OGN_data_txt", "w")
flm_ln = len(r.content) - 1
print "OGN db length: ", flm_ln
try:
    for i in range(0, flm_ln, 1):
        c = "%c" % data[i]
        flm.write(c)
except :
    print "Error writing OGN_data"   
    exit()
flm.close()

# Code modified to this point
# quit()

#db = open("data.fln", 'r')
db = open("OGN_data", 'r')
# Read first line and convert to number
x = db.readline()
# val = int(x, 16)
text = x.split(',')
print "First line is: ", text

try:
    # Creates or opens a file called mydb with a SQLite3 DB
    dbflarm = sqlite3.connect('flogger.sql3')
    print "Create OGN_db table in flogger.sql3"
    # Get a cursor object
    cursor = dbflarm.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS
                        flarm_db(id INTEGER, flarm_id TEXT PRIMARY KEY, airport STRING, type TEXT, registration TEXT, radio TEXT)''')
    # Commit the changes
#    dbflarm.commit()
# Catch the exception
except Exception as e:
    # Roll back any change if something goes wrong
    print "Failed to create flarm_db"
    dbflarm.rollback()
    raise e

# Start processing file content
#DEVICE_TYPE,DEVICE_ID,AIRCRAFT_MODEL,REGISTRATION,CN,TRACKED,IDENTIFIED

i = 1
line = ""
# nos_lines = val 
while True:
    try:
	line = db.readline()
	line = line.replace("'","")
	text = line.split(',')
	text[6] = text[6].replace("\r","")
        text[6] = text[6].replace("\n","")
        string = ""
	# print text
        # print "read: ", i, " returns: ", text
        
	# Check if tracking and identification is allowd in record.
	if text[5] == "N":
		print text[1],' does not want to be tracked'
	if text[6] == "N":
		print text[1]," does not want to be identified"
	
	# Check if it is a valid message to process
	if (text[0] == "F" or text[0] == "O" or text[0] == "I") and text[5] == "Y" and text[6] == "Y":
		# Process Flarm messages here
		# print 'Flarm/OGN/I message - start processing.'

		ID = text[1]
       		# OGN ddb does not contain Airport info. Left empty.
		Airport = ""
		Type = text[2]
        	Registration = text[3]
        	# OGN ddb does not contain Radio info. Left empty.
		Radio = ""
        	# print "Line: ", i-1, " ID: ", ID,  " Airport: ", Airport, " Type: ", Type, " Registration: ", Registration,  " Radio: ", Radio
       		row = "%s__%s__%s__%s__%s\n" % (ID, Airport, Type, Registration, Radio)
        	flm_txt.write(row)
        	
		# Check if entry is alread present
		try:
			cursor.execute('''SELECT count(flarm_id) from flarm_db where flarm_id = :flarm_id''',{'flarm_id':ID})
			test = cursor.fetchone() 
			# print test
			# print ID
			if test == (1,):
				# print "ID already present - Update."
				cursor.execute('''UPDATE flarm_db SET type = :type, registration = :registration WHERE flarm_id = :flarm_id ''', {'type': Type, 'registration': Registration, 'flarm_id': ID})
				# print 'Update complete'
				continue
			else:
				cursor.execute('''INSERT INTO flarm_db(flarm_id, type, registration)
                           VALUES(:flarm_id, :type, :registration)''',
                            {'flarm_id': ID, 'type': Type, 'registration': Registration})
		except:
			print "Something when wrong while updating flarm_d"

		# try:
            	# 	cursor.execute('''INSERT INTO flarm_db(flarm_id, type, registration)
                #           VALUES(:flarm_id, :type, :registration)''',
                #            {'flarm_id': ID, 'type': Type, 'registration': Registration})
        	# except :
           	# 	print "Flarm_db insert failed for following ID: ", ID
           	# 	dbflarm.commit()
	else:
		# Anything else
		print text
		print 'Anything else - do nothing.'
	
        
	
	i = i + 1
        
    except:
        print "Last line is: ", i
        dbflarm.commit()
        exit()
dbflarm.commit()
