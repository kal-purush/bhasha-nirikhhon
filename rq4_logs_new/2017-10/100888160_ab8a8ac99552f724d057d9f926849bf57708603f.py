from bs4 import BeautifulSoup
import requests
import time
import csv
import datetime

i = datetime.datetime.now()


i.strftime('%m.%d.%y')

print ','.join(['artist_name', i.strftime('%m.%d.%y')])
#for loop
with open('the_young_internet_nodes.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next (readCSV)
    for row in readCSV:
		source = row[2]
		parsed_source = source.split("/")
		artist_url = parsed_source[3]
		web_page = requests.get(source)

		soup = BeautifulSoup(web_page.content, "html.parser")

		#artist = soup.find('a', href= "/" + artist_url).text
		artist = row[1]
		meta = soup.find('meta', property="soundcloud:follower_count")
		followers = meta["content"]
		print ','.join([artist, followers]).encode('utf-8').strip()
		time.sleep(3)
