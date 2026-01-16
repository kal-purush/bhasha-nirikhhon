#Project libraries

import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine,text

# Connect to the MySQL database
connection = create_engine("mysql+mysqldb://globantuser:pruebatecnica123@localhost/process_globant")
conn = connection.connect()

# Departments data
path_dept= './departments.csv'
df_dept = pd.read_csv(path_dept, delimiter=',', header=None)
df_dept.columns =['iddepartments', 'departments_name']
df_dept['departments_name'] = df_dept['departments_name'].astype(pd.StringDtype())


# Jobs data
path_job= './jobs.csv'
df_job = pd.read_csv(path_job, delimiter=',', header=None)
df_job.columns =['idjobs', 'job_name']
df_job['job_name'] = df_job['job_name'].astype(pd.StringDtype())

# Hired Employees data
path_he= './hired_employees.csv'
df_he = pd.read_csv(path_he, delimiter=',', header=None)
df_he.columns =['idhired_employees', 'employee_name','hired_date','departments_code','job_code']
df_he['employee_name'] = df_he['employee_name'].astype(pd.StringDtype())
df_he['hired_date'] = pd.to_datetime(df_he['hired_date'])


#Table control
drop_table_dept = text(f"DROP TABLE IF EXISTS departments")
conn.execute(drop_table_dept)
drop_table_job = text(f"DROP TABLE IF EXISTS jobs")
conn.execute(drop_table_job)
drop_table_he = text(f"DROP TABLE IF EXISTS hired_employees")
conn.execute(drop_table_he)


#Data insertion into the database
df_dept.to_sql(name="departments", con=connection, if_exists="append", index=False, chunksize = 1000, method='multi')
df_job.to_sql(name="jobs", con=connection, if_exists="append", index=False, chunksize = 1000, method='multi')
df_he.to_sql(name="hired_employees", con=connection, if_exists="append", index=False, chunksize = 1000, method='multi')

#Close database conncetion
conn.close()