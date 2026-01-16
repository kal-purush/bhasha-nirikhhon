import sqlite3
import json

def list_tags():
    # Open a connection to the database
    with sqlite3.connect("./db.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        SELECT
            t.id,
            t.label
        FROM Tags t
        """)
        query_results = db_cursor.fetchall()

        # Initialize an empty list and then add each dictionary to it
        tags=[]
        for row in query_results:
            tags.append(dict(row))

        # Serialize Python list to JSON encoded string
        serialized_tags = json.dumps(tags)

    return serialized_tags


def retrieve_tag(pk):
    # Open a connection to the database
    with sqlite3.connect("./db.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        SELECT
            t.id,
            t.label
        FROM Tags t
        WHERE t.id = ?
        """, (pk,))
        query_results = db_cursor.fetchone()

        # Serialize Python list to JSON encoded string
        dictionary_version_of_object = dict(query_results)
        serialized_tag = json.dumps(dictionary_version_of_object)

    return serialized_tag


def create_tag(tag):
    # Open a connection to the database
    with sqlite3.connect('./db.sqlite3') as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        db_cursor.execute("""
        Insert INTO Tags (label) values (?)
        """, (
            tag['label'],
        ))

        id = db_cursor.lastrowid

        return json.dumps({
            'token': id,
            'valid': True
        })