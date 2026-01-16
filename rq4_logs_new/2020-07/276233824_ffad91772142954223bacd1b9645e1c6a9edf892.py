import pymysql
import cgi
import time


print("Content-Type: text/html") #HTML is following
print()
print("<html>")
print("<body>")

t = time.localtime()
current_time = time.strftime("%H",t)
if int(current_time) < 12 and int(current_time) >= 0:
	greeting_str = "Morning"
elif int(current_time) < 18 and int(current_time) >= 12:
	greeting_str = "Afternoon"
elif int(current_time) <= 23 and int(current_time) >= 18:
	greeting_str = "Evening"

print("<H1>Good " + greeting_str + "! Please Look at my Gallery!</H1>")
#link to go to 'View Galleries'
print("<a href='listGalleries.py'>View Galleries</a>")
#link to go to 'View Artists'
print("<a href='listArtists.py'>View Artists</a>")
#link to go to 'Search'
print("<a href='search.py'>Search</a>")