class Connenct(object):
    def __enter__(self):
        import sqlite3
        open= sqlite3.connect('test.db')
        self.connection=open
        return self.connection.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.connection.close()