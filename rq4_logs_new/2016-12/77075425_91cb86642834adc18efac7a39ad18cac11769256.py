# ReadDB v 0.0.1
# Francis Windram 2016
# Python 3.4
#
# Created:  21/12/16
# Modified: 21/12/16
#
# A Python3/SQL project for building and maintaining a book database.
#
# Dependencies:
#   - sqlite3
#   - isbnlib
#
# ===== TO DO LIST =====
# TODO - If extant, open SQLite db
# DONE - Else construct new DB on first run with required tables/columns
# TODO - ADD basic SQL logic for addition & deletion
# TODO - ADD SQL search functionality
# TODO - ADD auto-ISBN lookup for quick entry (using isbntools)
#       - isbnlib.meta() returns dict in the following style:
#     {
#         'Year':'yyyy',
#         'ISBN-13':'xxxxxxxxxxxxx',
#         'Publisher':'publisher',
#         'Authors':['Fore M. Sur', 'First Last'],
#         'Title':'The Title of the Book'
#         'Language':'language'
#     }
# TODO - ADD csv import function
# TODO - ADD error logging
# TODO - ADD tkinter interface


import sqlite3
import isbnlib


def create_db(recreate=False):
    """Creates DB with correct tables (drops table if required)"""
    connection = sqlite3.connect("books.sqlite")
    cursor = connection.cursor()
    if recreate:
        try:
            print("Dropping table...")
            cursor.execute("""DROP TABLE employee;""")
            print("Table dropped.")
        except sqlite3.OperationalError:
            print("\n!!! === No table present === !!!\n")
    dbcreate_cmd = """
    CREATE TABLE books (
    bookid INTEGER PRIMARY KEY,
    surname VARCHAR(30),
    forename VARCHAR(30),
    title VARCHAR(255),
    isbn INTEGER(13),
    publisher VARCHAR(100),
    year INTEGER,
    language VARCHAR(30));"""
    print("Creating table...")
    cursor.execute(dbcreate_cmd)
    print("Table created.")

    connection.commit()
    connection.close()