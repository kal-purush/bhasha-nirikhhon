import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="SMS"
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Define CREATE TABLE queries
create_table_queries = [
    """
    CREATE TABLE level_category (
        level_categoryID INT AUTO_INCREMENT PRIMARY KEY,
        level_category_name VARCHAR(255)
    )
    """
    ,
    """
    CREATE TABLE level (
        levelID INT AUTO_INCREMENT PRIMARY KEY,
        levelname VARCHAR(255),
        level_categoryID INT,
        FOREIGN KEY (level_categoryID) REFERENCES level_category(level_categoryID)
    )
    """
    ,
    """
    CREATE TABLE faculty (
        facultyID INT AUTO_INCREMENT PRIMARY KEY,
        facultyname VARCHAR(255),
        levelID INT,
        FOREIGN KEY (levelID) REFERENCES level(levelID)
    )
    
    """,
    """
    CREATE TABLE Class (
        classID INT AUTO_INCREMENT PRIMARY KEY,
        classname VARCHAR(255),
        facultyID INT,
        FOREIGN KEY (facultyID) REFERENCES faculty(facultyID)
    )
    
"""
]

# Execute each CREATE TABLE query
for query in create_table_queries:
    cursor.execute(query)

# Commit changes and close connection
conn.commit()
conn.close()