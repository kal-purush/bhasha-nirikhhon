import sqlite3
import json
from datetime import datetime


def new_comment(comment_data):
    with sqlite3.connect("./db.sqlite3") as conn:
        db_cursor = conn.cursor()
        db_cursor.execute(
            """
        INSERT INTO Comments(post_id, author_id, content)
        VALUES (?,?,?)  

        """,
            (
                comment_data["post_id"],
                comment_data["author_id"],
                comment_data["content"],
            ),
        )
        id = db_cursor.lastrowid

        return json.dumps({"token": id, "valid": True})


def get_comments_by_post_id(post_id):
    with sqlite3.connect("./db.sqlite3") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the post by ID
        db_cursor.execute(
            """
                SELECT
                    c.id,
                    c.post_id,
                    c.author_id,
                    c.content
                FROM Comments c
                WHERE c.post_id = ?
                """,
            (post_id,),
        )
        query_results = db_cursor.fetchall()

        comments = []
        for row in query_results:
            comment = {
                "id": row["id"],
                "post_id": row["post_id"],
                "author_id": row["author_id"],
                "content": row["content"],
            }
            comments.append(comment)
        # Serialize Python list to JSON encoded string
        serialized_comments = json.dumps(comments)

        return serialized_comments