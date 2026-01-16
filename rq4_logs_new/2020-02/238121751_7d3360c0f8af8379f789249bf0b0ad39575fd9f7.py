import sqlite3

class DatabaseQueries():

    def __init__(self):
        
        # connect to database
        self.database_connection = sqlite3.connect('repo.db')
        self.query = self.database_connection.cursor()

    def save_note(self, time, text, application_name):
        
        self.query.execute("insert into notes (time, text, application) values (?,?,?)",(time, text, application_name))
        # end database connection
        self.database_connection.commit()
        self.database_connection.close()

    def edit_note(self, note_id, note_time, note_text):
        
        self.query.execute("UPDATE notes SET time = ?, text = ? WHERE id = ?",(note_time, note_text, note_id))
        # end database connection
        self.database_connection.commit()
        self.database_connection.close()

    def delete_note(self, note_id):
        
        self.query.execute("DELETE FROM notes WHERE id = " + note_id)
        # end database connection
        self.database_connection.commit()
        self.database_connection.close()


    def get_notes_by_application(self, application_name):

        # show appplication specific notes
        self.query.execute("select * from notes where application = '" + application_name + "'")
        return self.query

    def get_note_by_id(self, note_id):

        # show appplication specific notes
        self.query.execute("select text from notes where id = " + note_id)
        for row in self.query:
            note_text = row[0]
        # end database connection
        self.database_connection.close()
        return note_text

    def get_application_list(self):
        self.query.execute("select distinct application from notes")
        apps = []
        for row in self.query:
            apps.append(row[0])
        self.database_connection.close()
        return apps
'''
config:
sqlite3 repo.db
create table notes (id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, text TEXT, application TEXT )
'''