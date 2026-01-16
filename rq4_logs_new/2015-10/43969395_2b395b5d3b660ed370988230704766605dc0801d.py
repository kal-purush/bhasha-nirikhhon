import sqlite3

class database:
    def __init__(self, database, schema):
        self.db = db = sqlite3.connect(database)
        self.c = c = db.cursor()
        print " !- Connecting to databse'" + database + "'..."
        if c.execute("SELECT sql FROM sqlite_master WHERE type='table';").fetchall() != schema:
            print " !- Invalid database schema! Dropping everything and recreating."
            for table in c.execute("SELECT name FROM sqlite_master WHERE type='table';"):
                print " !-- Dropping table " + table + "..."
                c.execute("DROP TABLE " + table[0] + ";")
            for statement in schema:
                print " !-- Execute: (" + statement[0] + ";)"
                c.execute(statement[0] + ";")
        else:
            print " !- Valid database schema. Carry on."
    
    def add_story(self, title, author, contents):
        self.c.execute("INSERT INTO stories VALUES(?, ?, ?);", (title, author, contents))
        self.db.commit()
    
    def get_stories(self):
        stories = []
        for story in self.c.execute("SELECT title FROM stories").fetchall():
            stories.append(story[0])
        return stories

# Example Usage:
# 
# import database from database
# db = database()
# db.addstory("Test", "Guy", "Thing")
# db.get_stories()