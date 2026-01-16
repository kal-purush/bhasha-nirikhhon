!pip install mysql-connector-python
import mysql.connector
import pandas as pd
pd.set_option('display.max_rows',None)
mydb = mysql.connector.connect(user='root', password='Mysql@2024', host='localhost')
mycursor=mydb.cursor()


#1
mycursor.execute('create database customer')


#2
import sqlite3
def connect_to_database(database_path):
    try:
        connection=sqlite3.connect(database_path)
        print("connection_successfull")
        return connection
    except:
        print("connection unsuccessfull")
database_path='Customer'

connection=connect_to_database(database_path)


#3
import sqlite3
def connect_to_database(database_path):
    try:
        connection=sqlite3.connect(database_path)
        print("connection_successfull")
        return connection
    except:
        print("connection unsuccessfull")
database_path='Customer'

connection=connect_to_database(database_path)
mydb = mysql.connector.connect(user='root', database='Customer',password='Mysql@2024', host='localhost')
mycursor=mydb.cursor()


#4
mycursor.execute(
    '''create table Customer_data 
    (Customer_id int Primary key,
    Customer_name varchar(200),
    Email_id varchar(250),
    City varchar(200))''')

    
#5
mycursor.execute(
    '''insert into Customer_data values
    (1, 'Alice', 'alice@gmail.com', 'New York'), 
    (2, 'Bob', 'bob@gmail.com', 'New York'), 
    (3, 'Rob', 'rob@gmail.com', 'San Francisco'), 
    (4, 'Emma', 'emma@gmail.com', 'San Francisco'), 
    (5, 'David', 'david@gmail.com', 'Denver')''' )


import pandas as pd


#6 
mycursor.execute("select Customer_name from customer_data where City='New York' ")
result=mycursor.fetchall()
print(result)


#7
mycursor.execute("update Customer_data set City = 'Austin' where Customer_name = 'Bob' ")


#8
mycursor.execute("delete from Customer_data where Customer_name = 'Alice'")

mycursor.execute("select * from Customer_data") 
mycursor.fetchall()


#9
mycursor.execute(
    '''create table purchase (
    purchase_id int primary key,
    product_name varchar(250) ,
    customer_id int,
    foreign key(customer_id) references customer_data(customer_id))''')

#9
mycursor.execute(
    ''' insert into purchase values
    (1, 'Phone', 4), 
    (2, 'Laptop', 2), 
    (3, 'Tablet', 3), 
    (4, 'Smartwatch', 4), 
    (5, 'Headphones', 2)''')


#10
mycursor.execute('''select c.customer_name,p.product_name from Customer_data c join purchase p on c.Customer_id=p.Customer_id''') 
result=mycursor.fetchall()
print(result)


#11
mycursor.execute('''select count(*) from purchase where product_name ='phone' ''')
mycursor.fetchall()


#12
mycursor.execute(''' select customer_id,count(*) as number_of_purchases from purchase group by customer_id order by number_of_purchases desc limit 2''')
mycursor.fetchall()
