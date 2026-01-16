import MySQLdb

class InvestDao:
    def __init__(self, host="127.0.0.1", port=3306, user="michal", passwd="michal12", db="invest_data"):
        self.conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    def sql(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
