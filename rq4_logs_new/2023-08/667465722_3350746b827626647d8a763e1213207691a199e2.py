"""
utility function for interacting with the database.
including:
1. connect to the database
2. create tavle kamus alay & abusive
3. insert record of data cleansing 
"""
import pandas as pd
import sqlite3

def create_connection():
    conn = sqlite3.connect('platinum_challenge.db')
    return conn 

# pembaharuan 1
def create_table_kamus_alay(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS new_kamusalay
    (alay_word TEXT, formal_word TEXT)''')
    conn.commit()
    print("Table new_kamusalay created successfully!")

def insert_dictionary_to_db(conn):
    abusive_csv_file = "csv_data/abusive.csv"
    alay_csv_file = "csv_data/new_kamusalay.csv"
    # Read csv file to dataframe
    print("Reading scv file to dataframe....")
    df_abusive = pd.read_csv(abusive_csv_file)
    df_alay = pd.read_csv(alay_csv_file)

    #Standardize column name
    df_abusive.columns =['word']
    df_alay.columns = ['alay_word', 'formal_word']

    # insert dataframe to database
    print('Inserting dataframe to database...')
    df_abusive.to_sql('abusive', conn, if_exists='replace', index=False)
    df_alay.to_sql('alay', conn, if_exists='replace', index=False)
    print("Inserting dataframe to database success!")

def insert_result_to_db(conn, raw_text, clean_text):
    # Insert result to database
    print("Inserting result to database...")
    df = pd.DataFrame({'raw_text': [raw_text], 'clean_text': [clean_text]})
    df.to_sql('cleansing_result', conn, if_exists='append', index=False)
    print("Insering result to database success!")

def insert_upload_result_to_db(conn, clean_df):
    # Insert result to database
    print("Inserting result to database...")
    clean_df.to_sql('cleansing_result', conn, if_exists='append', index=False)
    print("Insering result to database success!")

def show_cleansing_result(conn):
    # show cleansing result
    print("showing cleansing result...")
    df = pd.read_sql_query("SELECT * FROM cleansing_result",conn)
    return df.T.to_dict()

def get_abusive_data(conn):
    df = pd.read_sql_query('SELECT * FROM abusive', conn)
    return df

#pembaharuan 1
def get_alay_data(conn):
    df = pd.read_sql_query('SELECT * FROM new_kamusalay', conn)
    return df