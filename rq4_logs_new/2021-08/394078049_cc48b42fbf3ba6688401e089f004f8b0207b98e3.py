import mysql.connector
import keyring

def connectToDB():
    try:
        cnx = mysql.connector.connect(user='root',
                                      password=keyring.get_password("system","root"),
                                      database='foods')
        cursor = cnx.cursor()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()

def insertToDB():

    # attempt to connect to DB
    connectToDB()

    # insert statement
    add_foods = ("INSERT INTO foods "
                 "(prod_name, ss_size, chol, salt, sodium, carb, fiber, sugars, proteins) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")

    # TO DO
    # retrieve values
    data_foods =

    # insert new food and get last id
    cursor.execute(add_foods, data_foods)

    # commit
    cnx.commit()