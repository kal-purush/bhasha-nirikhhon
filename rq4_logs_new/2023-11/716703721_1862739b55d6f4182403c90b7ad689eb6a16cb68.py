import os
import psycopg2
from dotenv import load_dotenv
from contextlib import closing

# Load environment variables
load_dotenv()

# Database connection parameters
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Function to connect to the database
def connect_to_db():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

# Function to insert a new session
def insert_new_session(start_timestamp, language, data_period, nb_files):
    with closing(connect_to_db()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO session (start_timestamp, language, data_period, files_processed) VALUES (%s, %s, %s, %s) RETURNING session_id;",
                (start_timestamp, language, data_period, str(nb_files))
            )
            session_id = cursor.fetchone()[0]
            conn.commit()
            return session_id

# Function to update file progress
def create_file_progress(session_id, file_name, total):
    with closing(connect_to_db()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO file_progression (session_id, file_name, status, total) VALUES (%s, %s, %s, %s);",
                (session_id, file_name, "init", total)
            )
            conn.commit()

# Function to update file progress
def update_file_progress(session_id, file_name, progress):
    with closing(connect_to_db()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE file_progression SET status='progress', progress= %s WHERE session_id=%s AND filename=%s;",
                (progress, session_id, file_name)
            )
            conn.commit()

# Function to close a session
def close_session(session_id, end_timestamp, files_processed):
    with closing(connect_to_db()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE session SET end_timestamp = %s, files_processed = %s WHERE session_id = %s;",
                (end_timestamp, files_processed, session_id)
            )
            cursor.execute("UPDATE file_progression SET status='done' WHERE session_id=%s",(session_id))
            conn.commit()