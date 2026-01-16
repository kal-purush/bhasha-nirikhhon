import mysql.connector

#conexion a la base de datos
def get_connection():
    mydb = mysql.connector.connect(
        host="localhost",  
        user="root",  
        password="", 
        database="inventariio" 
    )
    return mydb