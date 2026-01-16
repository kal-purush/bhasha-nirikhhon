"""StoryBored SQLite database handler module.

Allows for database communication. Instantiate with:
    
    db = Database(<path to database>, <schema>)
    
Schemas should be formatted as follows:

    SCHEMA = [
        ('<table1>', [
            ('<col1>', 'type'),
            ('<col2>', 'type'),
            ...
        ]),
        ('<table2>', [
            ('<col1>', 'type'),
            ('<col2>', 'type'),
            ...
        ]),
        ...
    ]
"""

from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3

class Database:
    """StoryBored main database handler class.
    
    Attributes:
        db (sqlite3 connection): The sqlite3 connection to the database.
        c (sqlite3 Cursor): The Cursor that can be used to send statements to
            the sqlite3 database.
    
    """
    def __init__(self, database, schema):
        """StoryBored Database __init__.
        
        Args:
            database (str): Path to the sqlite3 database.
            schema (schema_statement): Schema to be applied to the
                database.
    
        If the database's schema does not match the one provided by the schema
        statement, all of the database's tables will be dropped and it will be
        recrated with the appropriate tables and data.
        """
        self.db = db = sqlite3.connect(database, check_same_thread=False)
        self.c = c = db.cursor()
        print " !- Connecting to databse'" + database + "'..."
        schema = format_schema(schema)
        if c.execute("SELECT sql FROM sqlite_master WHERE type='table';").fetchall() != schema:
            print " !- Invalid database schema! Dropping everything and recreating."
            for table in c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall():
                print " !-- Dropping table " + table[0] + "..."
                c.execute("DROP TABLE " + table[0] + ";")
            for statement in schema:
                print " !-- Execute: (" + statement[0] + ";)"
                c.execute(statement[0] + ";")
        else:
            print " !- Valid database schema. Carry on."
    
    def add_story(self, title, author, contents):
        self.c.execute("INSERT INTO stories VALUES(?, ?, ?, ?);", (title, str(author), contents, -1))
        self.db.commit()
        return self.c.lastrowid
        
    def update_story_link(self, story, link):
        self.c.execute("UPDATE stories SET link=(?) WHERE rowid=(?);", (link, story))
        self.db.commit()

    def get_next_rowid(self):
        return self.c.execute("select max(rowid) from stories;")
        #self.db.commit()
    
    def add_user(self, username, password):
        password = generate_password_hash(password)
        self.c.execute("INSERT INTO users VALUES(?, ?);", (username, password))
        self.db.commit()

    def update_user_password(self, username, new_password):
        password = generate_password_hash(new_password)
        self.c.execute("UPDATE users SET password=(?) WHERE username=(?);", (password, username))
        self.db.commit()
    
    def get_titles(self):
        return parse_simple_selection(self.c.execute("SELECT title FROM stories;").fetchall())
    
    def get_content(self):
        return parse_simple_selection(self.c.execute("SELECT contents FROM stories;").fetchall())

    def get_users(self):
        return parse_simple_selection(self.c.execute("SELECT username FROM users;").fetchall())

    def check_user_password(self, username, password):
        dat = self.c.execute("SELECT rowid, password FROM users WHERE username=(?);", (username,)).fetchone()
        if dat and check_password_hash(dat[1], password):
            return dat[0]
        return 0;
    
    def get_user_by_id(self, userid):
        return self.c.execute("SELECT username FROM users WHERE rowid=(?);", str(userid)).fetchone()[0] 

def parse_simple_selection(output):
    members = []
    for member in output:
        members.append(member[0])
    return members

def format_schema(schema):
    output_match = []
    for table in schema:
        statement = u'CREATE TABLE ' + table[0] + ' ('
        for col in table[1]:
            statement += col[0] + ' ' + col[1] + ', '
        statement = statement[:-2] + ')'
        output_match.append((statement,))
    return output_match