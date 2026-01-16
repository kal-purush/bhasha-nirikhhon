import sqlite3

class DbController:
    def __init__(self, dbFilepath):

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        self.dbFilepath = dbFilepath
        self.con = sqlite3.connect(self.dbFilepath)
        self.con.row_factory = dict_factory
        self.cur = self.con.cursor()

    def __del__(self):
        self.con.close()

    def execute(self, query, arg=None):
        try:
            if arg is None:
                self.cur.execute(query)
            else:
                self.cur.execute(query, arg)
            self.con.commit()
        except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
            print(e)
            return False
        return True
    
    def exist_fasta(self, _id):
        query = "SELECT EXISTS(SELECT 1 from fasta WHERE fasta_id=?)"
        success = self.execute(query, (_id,))
        if success:
            return list(self.cur.fetchone().values())[0]
        else:
            return -1
    
    def insert_fasta(self, _id, origin):
        query = "INSERT INTO fasta VALUES(?, ?, CURRENT_TIMESTAMP)"
        success = self.execute(query, (_id, origin))
        return success
    
    def delete_fasta(self, _id):
        query = "DELETE FROM fasta WHERE fasta_id=?"
        success = self.execute(query, (_id,))
        return success