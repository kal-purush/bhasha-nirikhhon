# -*-
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="asjsasa",
  password="hashsaj"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE clientes")
#If this page is executed with no error, you have successfully created a database.
    