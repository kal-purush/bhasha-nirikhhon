#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import threading
import psycopg2
import os
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (it, scn, ot, con):
	cur = con.cursor()
	q = "SELECT "+scn+" FROM "+it
	cur.execute(q)
	rows = [row[0] for row in cur]
	maxnum = max(rows)
	minnum = min(rows)
	if minnum == 0:	
		n = maxnum-minnum
	else:
		n = maxnum - minnum + 1
	rangesum = float(n/5)
	
	loadpartitions1(rangesum, it, con, scn, ot, maxnum, minnum)
	'''
	for i in range(0,5):
		cur.execute("SELECT * FROM "+it+str(i))
		print cur.fetchall()
	'''	
	thread_create(rangesum, it, con, scn, ot)
	
	cur.close()
	con.commit()

def loadpartitions1(rs, it, con, scn, ot, maxnum, minnum):
	cur = con.cursor()
	sum1 = 0
	cur.execute("select column_name,data_type from information_schema.columns where table_name = '"+it+"';")
	rows = cur.fetchall()
	n = len(rows)
	colnames = [r[0] for r in rows]
	datatypes = [r[1] for r in rows]
	for j in range(0,5):
		pname = it+str(j)
		query = "DROP TABLE IF EXISTS "+pname
		cur.execute(query)
		query = "CREATE TABLE "+pname+" ("
		for i in range(0,n):
			if i < n-1:
				query += colnames[i]+" "+datatypes[i]+","
			else:
				query += colnames[i]+" "+datatypes[i]		
		query += ");"
		cur.execute(query)
	cur.execute("SELECT * FROM "+it)
	r = cur.fetchall()
	n = len(r)
	cur.execute("SELECT "+scn+" FROM "+it)
	r2 = cur.fetchall()	
	scnnames = [r22[0] for r22 in r2]
	k = 0
	tname = ""
	#print rs
	for i in range(0,n):
		value = scnnames[i]
		for k in range(0,5):
			if value > float(minnum)+float(k*rs) and value <= float(minnum)+float((k+1)*rs):
				tname=it+str(k)
				break						
			elif value == minnum:
				tname=it+str(0)
				break
			k+=1
		if tname == "":
			tname = it+str(4)
		q = "INSERT INTO "+tname+" VALUES "+str(r[i])
		cur.execute(q)
		i += 1
		tname = ""
	
	cur.close()
	con.commit()

def thread_create(rs, it, con, scn, ot):
	cur = con.cursor()
	cur.execute("select column_name,data_type from information_schema.columns where table_name = '"+it+"';")
	rows = cur.fetchall()
	#print rows
	colnames = [r[0] for r in rows]
	datatypes = [r[1] for r in rows]
	query = "DROP TABLE IF EXISTS "+ot
	#print colnames
	#print datatypes
	#print query
	cur.execute(query)
	query2 = "CREATE TABLE parallelSortOutputTable ("
	n = len(rows)
	#print n	
	for i in range(0,n):
		if i < n-1:
			query2 += colnames[i]+" "+datatypes[i]+","
		else:
			query2 += colnames[i]+" "+datatypes[i]		
	query2 += ");"
	#print query2
	cur.execute(query2)
	
	thread = [0,0,0,0,0]
	for i in range(0,5):
		thread[i] = threading.Thread(target=sortfunction, args=(it, scn, i, con, ot))
		thread[i].start()
	for i in range(0,5):
		thread[i].join()

	cur.execute("SELECT * FROM "+it)
	colnames = [desc[0] for desc in cur.description]
	q1 = "INSERT INTO "+ot+" ("
	i = 0
	n = len(colnames)
	for c in colnames:
		if i < n-1:
			q1 += c+", "
		else:
			q1 += c
		i+=1
	q1 += ") VALUES ("
	for i in range(0,n):
		q1 += "%s,"
	q1 = q1[:-1]	
	q1 += ")"
	for i in range(0,5):
		
		cur.execute("SELECT * FROM "+it+str(i)+" ORDER BY "+scn+", "+colnames[0])
		rows = cur.fetchall()
		#print rows
		cur.executemany(q1, rows)
	cur.close()
	con.commit()

def sortfunction(it, scn, i, con, ot):
	cur = con.cursor()
	cur.execute("SELECT * FROM "+it)
	colnames = [desc[0] for desc in cur.description]
	newtabname = it+str(i)
	q = "SELECT * FROM "+newtabname+" ORDER BY "+scn+", "+colnames[0]
	#print q	
	cur.execute(q)	
	rows = cur.fetchall()
	#print rows
	q = "DELETE FROM "+newtabname
	cur.execute(q)
	q1 = "INSERT INTO "+newtabname+" ("
	i = 0
	n = len(colnames)
	for c in colnames:
		if i < n-1:
			q1 += c+", "
		else:
			q1 += c
		i+=1
	q1 += ") VALUES ("
	for i in range(0,n):
		q1 += "%s,"
	q1 = q1[:-1]	
	q1 += ")"
	cur.executemany(q1, rows)
	cur.close()
	con.commit()
	

########################################################################################################################

def sortfunction22(it1, it2, jc1, jc2, ot, i, colnames1, colnames2, con):
	cur = con.cursor()
	newtabname1 = it1+str(i)
	newtabname2 = it2+str(i)
	cur.execute("SELECT * FROM "+newtabname1)
	l1 = cur.fetchall()
	cur.execute("SELECT * FROM "+newtabname2)
	l2 = cur.fetchall()
	if len(l1) > 0 and len(l2) > 0:	
		cur.execute("SELECT * FROM "+newtabname1+" INNER JOIN "+newtabname2+" ON "+newtabname1+"."+jc1+" = "+newtabname2+"."+jc2)
		#print q	
		rows = cur.fetchall()
		print rows
	
		q1 = "INSERT INTO "+ot+" ("
		i = 0
		n = len(colnames1)+len(colnames2)
		for c in colnames1:
			q1 += it1+"_"+c+","
		
		for c in colnames2:
			if i < len(colnames2)-1:
				q1 += it2+"_"+c+","
			else:
				q1 += it2+"_"+c
			i+=1
		q1 += ") VALUES ("
		for i in range(0,n):
			q1 += "%s,"
		q1 = q1[:-1]	
		q1 += ")"
		#%s, %s, %s)"
		#print q
		cur.executemany(q1, rows) 		
	cur.close()
	con.commit()

def thread_create2(rs, it1, it2, con, jc1, jc2, ot):
	#print ot	
	cur = con.cursor()
	cur.execute("select column_name,data_type from information_schema.columns where table_name = '"+it1+"';")
	rows = cur.fetchall()
	#print rows
	colnames1 = [r[0] for r in rows]
	datatypes1 = [r[1] for r in rows]
	cur.execute("select column_name,data_type from information_schema.columns where table_name = '"+it2+"';")
	rows = cur.fetchall()
	#print colnames1
	colnames2 = [r[0] for r in rows]
	datatypes2 = [r[1] for r in rows]
	#print colnames2	
	#index1 = colnames1.index(jc1)
	#index2 = colnames2.index(jc2)
	#print index1
	#print index2
	cur.execute("DROP TABLE IF EXISTS parallelJoinOutputTable")
	query2 = "CREATE TABLE parallelJoinOutputTable ("
	n1 = len(colnames1)
	n2 = len(colnames2)
	#print n2
	i = 0
	for i in range(0,n1):
		query2 += it1+"_"+colnames1[i]+" "+datatypes1[i]+","
		i += 1
	
	i = 0
	for i in range(0,n2):
		query2 += it2+"_"+colnames2[i]+" "+datatypes2[i]+","
			
		i += 1
	query2 = query2[:-1]
	
	query2 += ");"
	cols = n1+n2-1
	#print query2
	cur.execute(query2) 
	
	thread = [0,0,0,0,0]
	for i in range(0,5):
		thread[i] = threading.Thread(target=sortfunction22, args=(it1, it2, jc1, jc2, ot, i, colnames1, colnames2, con))
		thread[i].start()
	for i in range(0,5):
		thread[i].join()		
	#cur.execute("SELECT * FROM "+ot)
	#rows = cur.fetchall()
	#print len(rows)
	#sortfunction22(it1, it2, jc1, jc2, ot, 0, colnames1, colnames2)
	cur.close()
	con.commit()
	
def ParallelJoin (it1, it2, jc1, jc2, ot, con):
	cur = con.cursor()
	cur.execute("select column_name,data_type from information_schema.columns where table_name = '"+it1+"';")
	rows = cur.fetchall()
	#print rows
	colnames1 = [r[0] for r in rows]
	datatypes1 = [r[1] for r in rows]
	cur.execute("select column_name,data_type from information_schema.columns where table_name = '"+it2+"';")
	rows = cur.fetchall()
	colnames2 = [r[0] for r in rows]
	datatypes2 = [r[1] for r in rows]
	#index1 = colnames1.index(jc1)
	#index2 = colnames2.index(jc2)
	cur.execute("DROP TABLE IF EXISTS parallelJoinOutputTable")
	query2 = "CREATE TABLE parallelJoinOutputTable ("
	n1 = len(colnames1)
	n2 = len(colnames2)
	i = 0
	for i in range(0,n1):
		query2 += it1+"_"+colnames1[i]+" "+datatypes1[i]+","
		i += 1
	
	i = 0
	for i in range(0,n2):
		query2 += it2+"_"+colnames2[i]+" "+datatypes2[i]+","
		i += 1
	query2 = query2[:-1]
	query2 += ");"
	#print query2
	cur.execute(query2) 

	q = "SELECT "+jc1+" FROM "+it1
	cur.execute(q)
	rows = [row[0] for row in cur]
	maxnum1 = max(rows)
	minnum1 = min(rows)
	q = "SELECT "+jc2+" FROM "+it2
	cur.execute(q)
	rows = [row[0] for row in cur]
	maxnum2 = max(rows)
	minnum2 = min(rows)
	maxnum = max(maxnum1, maxnum2)
	minnum = min(minnum1, minnum2)
	n = maxnum - minnum 
	rangesum = float(n/5)
	loadpartitions1(rangesum, it1, con, jc1, ot, maxnum, minnum)
	'''
	for i in range(0,5):
		cur.execute("SELECT * FROM "+it1+str(i))
		print cur.fetchall()
	'''

	loadpartitions1(rangesum, it2, con, jc2, ot, maxnum, minnum)
	'''
	for i in range(0,5):
		cur.execute("SELECT * FROM "+it2+str(i))
		print cur.fetchall()
	'''
	thread_create2(rangesum, it1, it2, con, jc1, jc2, ot)
	cur.close()
	con.commit()

	
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail