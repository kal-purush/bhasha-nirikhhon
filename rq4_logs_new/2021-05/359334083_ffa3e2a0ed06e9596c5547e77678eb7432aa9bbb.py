import mysql.connector

DBhost='localhost'        
DBdatabase='learn_pon'#資料庫
DBuser='root'      #帳號
DBpassword='HsuanHao_0610'#密碼
# DBpassword='root'#密碼

connection = mysql.connector.connect(
host=DBhost,         
database=DBdatabase, 
user=DBuser,      
password=DBpassword) 

sql = '''CREATE TABLE  membertable  (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    useremail VARCHAR(255) NOT NULL,
    userpassword VARCHAR(25) NOT NULL,
    time datetime DEFAULT CURRENT_TIMESTAMP);'''
    #   ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

cursor = connection.cursor()
cursor.execute(sql)
connection.commit()

cursor.close()
connection.close()


