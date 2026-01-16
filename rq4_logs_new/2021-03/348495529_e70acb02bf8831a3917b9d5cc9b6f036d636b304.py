import pymssql
import os

sqlServer = os.getenv('SQL_SERVER', 'none')
sqlUser = os.getenv('SQL_USER', 'none')
sqlPass = os.getenv('SQL_PASS', 'none')
sqlDb = os.getenv('SQL_DB', 'none')

def db():
    return pymssql.connect(server=sqlServer, user=sqlUser, password=sqlPass, database=sqlDb)

def mySqlQuery(query, one=False):
    cur = db().cursor()
    cur.execute(query)
    r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.connection.close()
    return (r[0] if r else None) if one else r
