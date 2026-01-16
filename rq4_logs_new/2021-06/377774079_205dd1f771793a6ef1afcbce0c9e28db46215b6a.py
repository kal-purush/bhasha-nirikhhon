import os
from os.path import dirname, join

import pymysql
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(verbose=True)

HOST = os.environ.get('TP_DB_HOST')
USER_ID = os.environ.get('TP_DB_USER')
PASSWORD = os.environ.get('TP_DB_PASSWORD')
DB_NAME = os.environ.get('TP_DB_DATABASE')
DB_CHARSET = os.environ.get('TP_DB_CHARSET')
DB_PORT = os.environ.get('TP_DB_PORT')


class DbHandler:
    '''
        디비와 통신을 하는 class
    '''

    def __init__(self):
        global pool
        global truepool

    def conn(self):
        pool = pymysql.connect(
            user=USER_ID,
            passwd=PASSWORD,
            host=HOST,
            db=DB_NAME,
            charset=DB_CHARSET,
            port=int(DB_PORT),
        )
        return pool

    def get_stream_date(self, stream_id):
        select_query = 'SELECT startDate FROM Streams WHERE streamId = %s'
        connection = self.conn()
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(select_query, stream_id)
        data = cursor.fetchall()
        data = data[0]['startDate']
        cursor.close()
        connection.close()
        return data