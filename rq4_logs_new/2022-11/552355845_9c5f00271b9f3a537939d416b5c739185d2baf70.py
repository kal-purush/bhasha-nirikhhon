import requests
from bs4 import BeautifulSoup
from csv import writer

url = 'https://www.pararius.com/apartments/amsterdam'

page = requests.get(url)
bowl = BeautifulSoup(page.content, 'html.parser')

dataloop = bowl.find_all('section', class_="listing-search-item")
with open('datastore.csv', 'w', encoding='utf-8', newline='') as f:
    the_writer = writer(f)
    header = ['name', 'price']
    the_writer.writerow(header)
    for data in dataloop:
        name = data.find('h2', class_="listing-search-item__title").text.replace("\n", '')
        price = data.find('div', class_="listing-search-item__price").text.replace("\n", '')
        info = [name, price]
        the_writer.writerow(info)