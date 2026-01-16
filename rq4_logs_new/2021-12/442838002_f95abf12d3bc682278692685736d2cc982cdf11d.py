import sqlite3

class DB:
    def __init__(self):
        self.create_reminder_table()
        
    def create_table(self, cur, name, cols):
        query = f'CREATE TABLE IF NOT EXISTS {name} {cols}'
        cur.execute(query)

    def create_reminder_table(self):
        db = sqlite3.connect('reminders_app.db')
        cur = db.cursor()

        try:
            cols = """(
                    reminder TEXT NOT NULL,
                    date TIMESTAMP
                )"""
            
            self.create_table(cur, 'reminder', cols)
            print("Table 'reminder' successfully created.")
        except sqlite3.Error as error:
            print('Could not create table', error)

        db.commit()
        db.close()

    def save_reminder(self, **data):
        db = sqlite3.connect('reminders_app.db')
        cur = db.cursor()

        try:
            query = f'INSERT INTO reminder VALUES (:reminder, :date)'
            cur.execute(query, {
                'reminder': data['reminder'],
                'date': data['date']
            })

            print('Reminder successfully saved.')
        except sqlite3.Error as error:
            print('Could not insert into table', error)

        db.commit()
        db.close()