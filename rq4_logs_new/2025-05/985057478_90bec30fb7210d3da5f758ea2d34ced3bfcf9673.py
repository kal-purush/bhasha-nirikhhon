#!/usr/bin/env python3
import dotenv
import os
import random
import mysql.connector
from mysql.connector import errorcode

dotenv.load_dotenv(".env")
DB_NAME = "users"

TABLE_DEFINITION = """
CREATE TABLE IF NOT EXISTS students (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name  VARCHAR(50) NOT NULL,
    last_name   VARCHAR(50) NOT NULL,
    year        VARCHAR(10) NOT NULL,
    program     VARCHAR(100) NOT NULL,
    university  VARCHAR(100) NOT NULL,
    notes       TEXT
) ENGINE=InnoDB;
"""

STUDENTS = [
    ("Lily",    "Zhang",      "2B", "Software Engineering", "University of Waterloo"),
    ("Mikayla", "Mao",        "2B", "Software Engineering", "University of Waterloo"),
    ("Sally",   "Jeong",      "2B", "Software Engineering", "University of Waterloo"),
    ("Rita",    "Xiang",      "2B", "Software Engineering", "University of Waterloo"),
    ("Bokang",  "Mogomotsi",  "2B", "Software Engineering", "University of Waterloo"),
]

RANDOM_NOTES = [
    "Enjoys hackathons and bubble tea.",
    "Varsity badminton player.",
    "Research assistant for the HCI lab.",
    "Co-op term at Shopify this fall.",
    "Building a side project in Rust + WASM.",
]


def connection():
    """Returns a MySQL connection using env-vars (or reasonable defaults)."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", ""),
        autocommit=True,
    )


def ensure_database(cursor):
    """Create the sample database if it doesn’t already exist."""
    try:
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {DB_NAME} "
            "DEFAULT CHARACTER SET 'utf8mb4'"
        )
    except mysql.connector.Error as err:
        print(f"[ERROR] Could not create database: {err}")
        raise


def main():
    cnx = connection()
    cur = cnx.cursor()
    ensure_database(cur)

    cur.execute(f"USE {DB_NAME}")
    cur.execute(TABLE_DEFINITION)

    add_student = """
        INSERT INTO students
        (first_name, last_name, year, program, university, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    cur.execute("DELETE FROM students")

    for s in STUDENTS:
        cur.execute(add_student, (*s, random.choice(RANDOM_NOTES)))

    cur.execute("SELECT first_name, last_name, year, program, university, notes FROM students")
    print("\n📋  Current rows in 'students':\n")
    for row in cur.fetchall():
        print(row)

    cur.close()
    cnx.close()


if __name__ == "__main__":
    main()