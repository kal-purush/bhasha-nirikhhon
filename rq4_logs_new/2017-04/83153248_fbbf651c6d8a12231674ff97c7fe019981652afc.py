
import psycopg2
from sys import argv

#connect to database
conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()

#checking for valid input, if valid, proceed to generate histogram 
if len(argv) != 3:
	print 'ERROR: Histogram only accepts 2 input parameters!'
if int(argv[2]) < int(argv[1]): 
	print 'ERROR: Invalid input; must be an interval [k1, k2]!'
 
else: 
	cur.execute("SELECT word, count FROM tweetwordcount WHERE count >= %s and count <= %s ORDER BY count DESC", (int(argv[1]), int(argv[2])))
	results = cur.fetchall()
	for word in results:
		print '%s: %d' %(word[0], word[1])

cur.close()
conn.close()


