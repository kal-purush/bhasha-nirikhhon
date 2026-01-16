import requests
from bs4 import BeautifulSoup


url = 'https://esstu.ru/index.htm'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
response = requests.get(url, headers=headers)
print(response.status_code)
soup = BeautifulSoup(response.content, "html.parser")
# nazvanie = soup.find('div', {'class' :'anime-title'}).find('h1')
# print ('Аниме: ' + nazvanie.text)
news = soup.find('div', class_= 'index-category-text index-category-text-active').find_all('a', class_= 'c3')
for item in news:
        print (item.text)