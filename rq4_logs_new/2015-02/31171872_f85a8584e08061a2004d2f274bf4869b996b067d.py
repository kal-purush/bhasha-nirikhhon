#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Write a csv file to sql lite data base if it exists and if the file exists
For the first one, you will need to create the db, dropping a db of the same name
Furthermore, comment out those lines (see try section in write_csv_to_db), and 
just use the insertion statement
"""

import sqlite3
import csv
import sys
import requests

def databaseconnection(databasename):

    """
    Creates a connection to an existing sql lite database
    Returns a cursor on the connection
    """

    # default for the connection parameter
    con = None

    try:
        # create a connection object called con
        con = sqlite3.connect(databasename)
        cur = con.cursor()

    except sqlite3.Error, e:

        if con:
            con.rollback()

        # if a connection can't be made, print an error with the arguements included
        print "Error %s:" % e.args[0]
        sys.exit(1)

    # return the cursor object    
    return (cur, con)

def write_csv_to_database(cursor, connection, tablename, csvfille):

    """
    Writes the csv file, row by row, into a database (table)
    """
    r = requests.get(csvfile)
    data = r.text
    datareader = csv.reader(data.decode('utf8').splitlines())
    datareader.next()
    datareader.next()
    headers = datareader.next()
    newhead = [str(i).strip('"') + " TEXT," for i in headers]
    newhead2 = ''.join(newhead)
    newhead2 = newhead2[0:-1]
    builder_query = "CREATE TABLE "+tablename+ " (" + ''.join(newhead2) + ")"
    bob = len(headers)-1
    j = str(builder_query)
    insertion_query = "INSERT INTO "+tablename+" VALUES(" + "?,"*bob + "?);"
    jj = str(insertion_query)
    
    
    datareader.next()
    datareader.next()
    
    try: 
        cur.execute("DROP TABLE IF EXISTS " + tablename)
        cur.execute(j)

        # open the new file to read from 
        #for row in datareader:
        x = [tuple(row) for row in datareader]
        #print x
        cur.executemany(jj, x)
               
        con.commit()
    
    except sqlite3.Error, e:

        if con:
            con.rollback()
    

        print "Error %s:" % e.args[0]
        sys.exit(1)

    finally:

        if con:
            con.close() 

        print "Finished Transaction"


if __name__ == "__main__":

    print """ Thank you for using the csv-to-litedb utility. \n

    We hope you typed in the CSVFILE first, then the TABLENAME, \n
    and then the DATABASE name.

    Have a nice day!
    """
    try:
        csvfile = sys.argv[1]
    except Exception:
        print "please enter a csvfile"
    
    try:
        tablename = sys.argv[2]
    except Exception:
        print "please enter a table name in the db"
    
    try:
        databasename = sys.argv[3]
    except Exception:
        print "please emter a database name"

    (cur, con) = databaseconnection(databasename)
    write_csv_to_database(cur, con, tablename, csvfile)