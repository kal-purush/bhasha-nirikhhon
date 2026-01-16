from lxml import html
from bs4 import BeautifulSoup
import requests
import re
page = requests.get('https://www.mzv.cz/consulate.newyork/en/visa_and_consular_information/visa/approved_visa/long_term_visa_approved.html')

print(page.status_code)

soup = BeautifulSoup(page.content, 'html.parser')

article_body = soup.find('div', attrs={'class': 'article_body'})

ids = soup.find_all(string=re.compile('NEWY'))

sameDayIds = soup.find_all(string=re.compile('NEWY20171207'))

print("Visas approved on 12/07: " + '\n')
for i in sameDayIds:
	print(i + '\n')

for i in ids:
	if i == 'NEWY201712070014':
		print("Your visa has been approved!")
		exit()

print('Your visa (NEWY20170014) has not been approved yet.')