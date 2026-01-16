#plot.py

import psycopg2
import sys
#import matplotlib.pyplot as plt
conn = psycopg2.connect(database="tcount", user="postgres", password="", host="localhost", port="5432")


cur = conn.cursor()

# I couldnt get matplotlib to install so couldnt do the actul plot - so instead just showing the top 20

cur.execute("SELECT word,count from wordtable ORDER by count desc limit 20")
records = cur.fetchall()
for rec in records:
	print rec[0]
	print rec[1]
#plt.hist(records)
conn.commit()

conn.close()