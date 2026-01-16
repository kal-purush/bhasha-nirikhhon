import pandas as pd
import unittest

from database import PgCnHandler
from database import QueryRunner


dbname = "test"
tablename = "test"
username = "hahdawg"


class QueryTester(unittest.TestCase):

    def setUp(self):
        handler = PgCnHandler(dbname=dbname, username=username)
        query_runner = QueryRunner(cnhandler=handler)
        create_table = """
                       DROP TABLE IF EXISTS %s;
                       CREATE TABLE %s (name text, age int, birthdate date)
                       """ % (tablename, tablename)
        query_runner.exec_query(create_table)
        self.query_runner = query_runner

    @property
    def data(self):
        name = ["Marshall", "Tiana", "Portia"]
        age = [2, 5, 7]
        bdate = [pd.to_datetime(s) for s in ["7/1/2013", "2/28/2011", "11/11/2008"]]
        res = {"name": name, "age": age, "birthdate": bdate}
        return pd.DataFrame(res)

    def test_insert(self):
        from_source = self.data
        self.query_runner.sql_insert(data=from_source, tablename=tablename)
        from_dest = self.query_runner.sql_select("SELECT * FROM %s" % tablename)
        from_dest = from_dest[from_source.columns]
        from_dest["birthdate"] = pd.to_datetime(from_dest["birthdate"])
        assert (from_source == from_dest).all().all()

    def tearDown(self):
        sql = "DROP TABLE IF EXISTS %s" % tablename
        self.query_runner.exec_query(sql)
        