import sqlite3
import json

def new_category(request_body):
    with sqlite3.connect("./db.sqlite3") as conn:
        db_cursor = conn.cursor()
        db_cursor.execute(
            """
        INSERT INTO Categories(label)
        VALUES (?)  

        """,
            (
                request_body["label"],
            ),
        )
        id = db_cursor.lastrowid

        return json.dumps({"id": id, "label": request_body["label"]})

def list_categories():
    with sqlite3.connect("./db.sqlite3") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute("""
        SELECT
            label
        FROM Categories
        ORDER BY label ASC
        """)
        query_results = db_cursor.fetchall()

        allLabels = []
        for row in query_results:
            allLabels.append({'label': row[0]})
        
        serialized_Labels = json.dumps(allLabels)

    return serialized_Labels