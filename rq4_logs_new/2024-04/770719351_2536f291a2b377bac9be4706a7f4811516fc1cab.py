import select
import mysql.connector
from mysql.connector import errorcode

try:
  reservationConnection = mysql.connector.connect(   host="localhost",user="pwd12345",password="exampleUser",
        database="manga_db",
        port="4040"
    )

except mysql.connector.Error as err:
   if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print('Invalid credentials')
   elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print('Database not found')
   else:
      print('Cannot connect to database:', err)

else:
   	##gets user search

    print('Input Search ', end = '')
    search = input()

    ##gets a the category of search
    #search_type = getSpecifiedSearch()
    search_type = "title"
    ##get what table to look through
    if search_type == "title":
        table_name = "Manga"
    elif search_type == "Author":
        table_name = "Authors"
    elif search_type == "Genre":
        table_name = "Genre"
    elif search_type == "Theme":
        table_name = "Theme"

    searchCursor = reservationConnection.cursor()

    ##PAGE 2 SELECT CODE
    searchQuery = """
    SELECT m.imgurl, m.title
    FROM {} m
    WHERE m.title = %s
    OR m.title LIKE %s 
    OR m.title LIKE %s
    ORDER BY CASE
    WHEN m.title = %s then 1
    WHEN m.title LIKE %s then 2
    WHEN m.title LIKE %s then 3
    """.format(table_name)

    # Execute the query with user input as parameters
    searchCursor.execute(searchQuery, (search, f'{search}%', f'%{search}%', search, f'{search}%', f'%{search}%'))

    # Fetch the results

    result= select.fetchall()


    ##if statement to see if anything was selected
    if not result:
        print("search not found")
    else:
        for x in result:
            print(x)

    reservationConnection.commit()
    searchCursor.close()
    reservationConnection.close()