import os, sys
sys.path.append(os.path.abspath(os.path.join('..', '..', '..', 'backend')))
from db.database import Database

# Create new feedback
def create():
    connection = Database.get_connection()
    connection.execute((
        "INSERT INTO Feedback(email, name, publicationDate, comment)"
        " VALUES(?, ?, CURRENT_TIMESTAMP, ?)"),
        (email, name, comment))
    connection.commit()