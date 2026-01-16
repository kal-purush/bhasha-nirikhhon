import requests
from bs4 import BeautifulSoup

URL = "https://ads.tiktok.com/business/creativecenter/inspiration/topads/pc/en?region=LB"
respones = requests.get(URL)
soup = BeautifulSoup(respones.text,'lxml')

data = soup.find_all('div',class_="VideoPlayerCard_poster__akF4N index-mobile_poster__ptnFe")
for el in data:
    x = el.find('a').get_text()
    with open(f'tmp.txt','w') as file:
        file.write(x)