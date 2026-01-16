import requests 
from bs4 import BeautifulSoup 
r = requests.get('https://www.wikipedia.org/') 
html=r.text 
#print(html)
soup = BeautifulSoup(html,  'html.parser') 
#print(soup.prettify())
print(soup.title)
print(soup.find_all('a'))
	