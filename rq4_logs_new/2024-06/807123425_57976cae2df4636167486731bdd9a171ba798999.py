import sqlite3
import json
from datetime import datetime


def new_post(post_data):
    with sqlite3.connect("./db.sqlite3") as conn:
        db_cursor = conn.cursor()
        db_cursor.execute(
            """
        INSERT INTO Posts(user_id, category_id, title, publication_date, image_url, content, approved)
        VALUES (?,?,?,?,?,?,?)  

        """,
            (
                post_data["user_id"],
                post_data["category_id"],
                post_data["title"],
                post_data["publication_date"],
                post_data["image_url"],
                post_data["content"],
                post_data["approved"],
            ),
        )
        id = db_cursor.lastrowid

        return json.dumps({"token": id, "valid": True})