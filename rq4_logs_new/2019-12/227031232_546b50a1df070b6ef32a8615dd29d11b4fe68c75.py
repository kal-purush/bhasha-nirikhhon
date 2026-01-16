import sqlite3


def query(x, database):
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    curs.execute(x)
    answer = curs.fetchall()
    curs.close()
    conn.commit()
    return answer