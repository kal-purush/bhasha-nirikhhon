# Import file from your system
file_path = input("Enter the path to the file: ")
print("Selected file:", file_path)


"""### **2. Connect with SQLite Databases"""

# Connect with sqlite database
# Import necessary libraries
import sqlite3

database = 'database.sqlite'

conn = sqlite3.connect(database)
print('Opened data successfully')

# Read SQL query for getting all the tables of database into a dataframe
# Here SELECT * means select all
import pandas as pd
tables = pd.read_sql("""SELECT *
    FROM sqlite_master
    WHERE type='table';""", conn)
tables



