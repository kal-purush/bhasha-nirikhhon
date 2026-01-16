import requests
from bs4 import BeautifulSoup
import sys
import datetime
import re

url = 'https://fortune.yahoo.co.jp/12astro/ranking.html'
rest = requests.get(url)

soup = BeautifulSoup(rest.text, 'html.parser')
today = soup.find(class_='txt').text
print(today + "の占い")

span_list = soup.find_all('span')
del span_list[0]
text_list = soup.find_all('p', class_='ft01')

rank = 0
for s,t in zip(span_list, text_list):
	rank+=1
	seiza = s.find('img').get('alt')
	print(str(rank) + "位　" + seiza+"　"+t.text)

print("以上、本日の占いでした。")

