from pprint import pprint
from lxml import html
import requests
from datetime import datetime
from pymongo import errors
from pymongo import MongoClient


def lentaru_news_parser():
    client = MongoClient('127.0.0.1', 27017)
    db = client['lentaru']
    news_collection = db.news_collection

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36 OPR/88.0.4412.40'}
    url = 'https://lenta.ru/'

    session = requests.Session()
    response = session.get(url, headers=header)
    dom = html.fromstring(response.text)

    posts = []
    items = dom.xpath("//div[@class='topnews']//a[contains(@class, 'card')]")

    for item in items[:]:
        post = {}
        title = item.xpath(".//h3/text() | .//span[@class='card-mini__title']/text()")
        link = item.xpath(".//@href")
        dates = item.xpath(".//time/text()")
        splitted_date = ''.join(dates).split(':')
        formatted_date = str(datetime.today().replace(hour=int(splitted_date[0]),
                                                      minute=int(splitted_date[1]),
                                                      second=0,
                                                      microsecond=0))

        post['source'] = 'Lenta.ru'
        post['title'] = title[0]
        post['link'] = url + link[0]
        post['date'] = formatted_date
        post['_id'] = str(link)
        posts.append(post)

        try:
            news_collection.insert_one(post)
        except errors.DuplicateKeyError:
            print(f' {post["_id"]} Already exists!')


if __name__ == "__main__":
    lentaru_news_parser()