import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime, date, timedelta
import ast

def sql_connection():
    try:
        con = sqlite3.connect(dbpath)                       #establish connection to database
        print ("Connection successfully established!")
        return con
    except Error as e:
        print(e)
        return None       #prints error if cannot establish connection

def cals_df(con, name):
    dframe = pd.read_sql(f'select date, {name} from cals', con, index_col='date')
    dframe.sort_index
    dframe.dropna(inplace=True)

    medians = []
    for i,key in enumerate(dframe.values):
        medians.append(ast.literal_eval(key[0]).get('median'))
    dframe['medians'] = medians
    dframe.drop(columns=[name], inplace=True)
    return dframe


if __name__ == '__main__':
    dbname = r'summit_picarro.sqlite'
    dbpath = r"C:\Users\ARL\Desktop\Summit_Databases_Sept2019\{}".format(dbname)
    con = sql_connection()

    print(cals_df(con, 'co_result'))
