from numpy import disp
import pyodbc
import mysql.connector
import pandas as pd

server = 'rafsanserve.database.windows.net,1433'
database = 'sfl_database'
username = 'mysflserver'
password = 'Mytimy2shine'


connection_engine = pyodbc.connect('Driver={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)



df = pd.read_csv("C:/Users/Rbhuiyan/Documents/My Resumes/SFL Scientific Interview/DATA.csv")

print(df.head())