import mysql.connector
import json
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="aztrus1",
        database="mydatabase"
        )
mycursor = mydb.cursor()
def deleteSQL():
    mycursor.execute("DROP TABLE YelpData")


def insertSQL():  
    
    #mycursor.execute("CREATE DATABASE mydatabase")
    mycursor.execute("CREATE TABLE YelpData (id INT AUTO_INCREMENT PRIMARY KEY, business_id VARCHAR(255), name VARCHAR(255), address VARCHAR(255), city VARCHAR(255), state VARCHAR(255), postal_code VARCHAR(255), latitude FLOAT(20,15), longitude FLOAT(20,15), stars INT, breakfast BOOLEAN, lunch BOOLEAN, dinner BOOLEAN, gluten_free BOOLEAN, dairy_free BOOLEAN, diabetic_friendly BOOLEAN, price_range INT, categories VARCHAR(255))")
    sql = "INSERT INTO YelpData (business_id, name, address, city, state, postal_code, latitude, longitude, stars, breakfast, lunch, dinner, gluten_free, dairy_free, diabetic_friendly, price_range, categories) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" 
    with open('indexedData.json', 'r', encoding='utf8') as indexedData:
        for jsonObjects in indexedData:
            tempDict = json.loads(jsonObjects)
            val = (tempDict["business_id"], tempDict["name"], tempDict["address"], tempDict["city"], tempDict["state"], tempDict["postal_code"], tempDict["latitude"], tempDict["longitude"], tempDict["stars"], tempDict["breakfast"], tempDict["lunch"], tempDict["dinner"], tempDict["gluten_free"], tempDict["dairy_free"], tempDict["diabetic_friendly"], tempDict["price_range"], ",".join(tempDict["categories"]))
            mycursor.execute(sql, val)
            mydb.commit()


def main():
    deleteSQL()
    insertSQL()
    

if __name__ == '__main__':
    main()