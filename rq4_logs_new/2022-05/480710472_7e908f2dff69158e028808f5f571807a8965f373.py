import sqlite3
from sqlite3 import Error
import pickle


def getcolunm(col1, col2, col3, save=False,table='restaurants', db_file='Groulette.db'):
    con1 = sqlite3.connect(db_file)
    # con1.row_factory = sqlite3.Row
    cur1 = con1.cursor()
    sql = f'select {col1}, {col2}, {col3} from {table}'
    cur1.execute(sql)

    rows = cur1.fetchall() 
    rating={}
    total_ratings = {}
    for row in rows:
        rating[row[0]] = row[1]
        total_ratings[row[0]] = row[2]
    con1.close()
    
    if save:
        with open('rating.pkl', 'wb') as f1:
            pickle.dump(rating, f1)
        with open('total_ratings.pkl', 'wb') as f2:
            pickle.dump(total_ratings, f2)

if __name__ == '__main__':
    # get rating and user_ratings_total from the table
    # getcolunm('place_id','rating', 'user_ratings_total')
    # load file
    rating = pickle.load(open('rating.pkl', 'rb'))
    total_ratings = pickle.load(open('total_ratings.pkl', 'rb'))
