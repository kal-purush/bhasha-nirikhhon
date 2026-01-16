__author__ = 'shaur'

import unittest
import os

from dbutils.models import MessageTable
from dbutils.managers import SQLiteDBManager


CURRDIR = os.path.basename(os.path.dirname(os.path.realpath(__file__)))
DBNAME = "test"
DBPATH = os.path.join(CURRDIR,DBNAME)

db_obj = None
db_table = None

def setUpModule():
    global db_obj,db_table
    db_table = MessageTable(DBNAME,CURRDIR)
    db_obj = SQLiteDBManager(db_table)
    db_obj.create_and_connect()


def tearDownModule():
    os.remove(db_table.get_db_path())
    os.rmdir(os.path.dirname(db_table.get_db_path()))


class MessageTableTest(unittest.TestCase):
    def test_insert(self):
        db_obj.run_query(db_table.get_insert_query(),[1,2,3,4])
        db_obj.run_query(db_table.get_insert_query(),[1,2,3,4])
        db_obj.run_query(db_table.get_insert_query(),[1,2,3,4])

        result = db_obj.run_query(db_table.get_get_query(),[])

        count = 0
        for row in result:
            count = count+1

        self.assertEqual(count,3)

    def test_empty(self):
        db_obj.run_query(db_table.get_insert_query(),[1,2,3,4])
        db_obj.run_query(db_table.get_insert_query(),[1,2,3,4])
        db_obj.run_query(db_table.get_insert_query(),[1,2,3,4])

        db_obj.clean_content()

        result = db_obj.run_query(db_table.get_get_query(),[])

        count = 0
        for row in result:
            count = count+1

        self.assertEqual(count,0)


if __name__ == '__main__':
    unittest.main()