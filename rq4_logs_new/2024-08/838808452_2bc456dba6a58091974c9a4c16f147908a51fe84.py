from nltk.corpus.reader import sqlite3, sys


SQLITE_DB_PATH = './corpus/corpus.db'

if __name__ == "__main__":

    id = str(input("id: "))

    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        query = ("SELECT sim FROM corpus WHERE id = '{}'".format(id))
        cursor.execute(query)
        entries = cursor.fetchall()
        if len(entries):
            sim = str(entries[0]).strip("'(,)")
            if sim == None or sim == "":
                raise Exception("No recommendations found")
            else:
                rec = sim.split(";")
                for i in range(10):
                    if i % 2 == 0:
                        print("============================================================")
                        print("id: {}".format(rec[i]))
                        query = "SELECT * FROM corpus WHERE id = '{}'".format(rec[i])
                        cursor.execute(query)
                        entry = cursor.fetchall()
                        print("title: ", entry[0][1])
                        print("author: ", entry[0][2])
                    else:
                        print("Similarity: {}".format(rec[i]))
        conn.close()

    except sqlite3.Error as e:
        print(e)
        sys.exit()
    except Exception as e:
        print(e) 

    
