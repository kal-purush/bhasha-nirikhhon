import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv

url = "http://quotes.toscrape.com/"

while True:
    res = requests.get(url)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'html.parser')
    quotes = soup.select('div[class="quote"]')
    for quote in quotes:
        quote_data = []
        quote_text = quote.select('span[class="text"]')[0].text
        quote_author = quote.select('small[class="author"]')[0].text
        quote_tags = quote.select('a[class="tag"]')
        quote_data.append(quote_text)
        quote_data.append(quote_author)
        for tag in quote_tags:
            quote_data.append(tag.text)
        with open("quotes.csv", "a") as file:
            writer=csv.writer(file, quoting=csv.QUOTE_ALL)
            writer.writerow(quote_data)
    next = soup.find("li", attrs={"class": "next"})

    if next:
        url = urljoin(url, next.find("a")["href"])
    else:
        break