import pymysql
import dbconfig
connection = pymysql.connect(host='localhost',
                             user=dbconfig.db_user,
                             passwd=db_password)

try:
        with connection.cursor() as cursor:
                    sql = "CREATE DATABASE IF NOT EXISTS restaurantmap"
                    cursor.execute(sql)
                    sql = """CREATE TABLE IF NOT EXISTS restaurantmap.restaurants  (
id int NOT NULL AUTO_INCREMENT,
latitude FLOAT(10,6),
longitude FLOAT(10,6),
date DATETIME,
catrgory VARCHAR(50),
description VARCHAR(500),
updated_at TIMESTAMP,
PRIMARY KEY (id)
)"""

                    cursor.execute(sql);
         conneciton.commit()
finally:
         connection.close