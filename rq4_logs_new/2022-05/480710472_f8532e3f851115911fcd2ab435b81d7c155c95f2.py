import sqlite3
from sqlite3 import Error
import pickle


def create_connection(db_file='Groulette.db'):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def create_table(db_file = 'Groulette.db'):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE IF NOT EXISTS restaurants
                (genre INT, 
                 rating REAL, 
                 user_ratings_total INT,
                 name NVARCHAR(100),
                 place_id VARCHAR(255),
                 vicinity NVARCHAR(100),
                 location VARCHAR(255),
                 PRIMARY KEY (place_id)
                 )''')
    print("Table is Ready")
                
    #commit the changes to db			
    conn.commit()
    #close the connection
    conn.close()

def insert_data(row, db_file='Groulette.db', tablename='restaurants'):
    # keys = ','.join(row.keys())
    # question_marks = ','.join(list('?'*len(row)))
    # values = tuple(row.values())
    # conn.execute('INSERT INTO '+tablename+' ('+keys+') VALUES ('+question_marks+')', values)

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("Successfully Connected to SQLite")

        keys = ','.join(row.keys())
        question_marks = ','.join(list('?'*len(row)))
        values = tuple(row.values())

        count = cursor.execute('INSERT INTO '+tablename+' ('+keys+') VALUES ('+question_marks+')', values)
        conn.commit()
        print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if conn:
            conn.close()
            print("The SQLite connection is closed")

def drop_table(tablename, db_file='Groulette.db'):
    #Connecting to sqlite
    conn = sqlite3.connect(db_file)

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Doping table if already exists
    cursor.execute("DROP TABLE {}".format(tablename))
    print("Table dropped... ")

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

'''
  - genre
  - rating
  - user_ratings_total
  - name
  - place_id
  - vicinity
  - location
'''

if __name__ == '__main__':
    # create_connection()
    # create_table()
    # drop_table('restaurants')

    with open('restaurant.pkl', 'rb') as f:
        restaurants = pickle.load(f)

    for genre in restaurants:
        for restaurant in genre:
            insert_data(restaurant)
        



