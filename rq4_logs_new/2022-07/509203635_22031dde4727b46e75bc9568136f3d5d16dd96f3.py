from pprint import pprint
from lxml import html
import requests
from datetime import datetime
from pymongo import errors
from pymongo import MongoClient


def mailru_news_parser():
    client = MongoClient('127.0.0.1', 27017)
    db = client['mailru']
    news_collection = db.news_collection

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36 OPR/88.0.4412.40'}

    url = 'https://news.mail.ru/'

    session = requests.Session()
    response = session.get(url, headers=header)
    dom = html.fromstring(response.text)

    links = set(dom.xpath("//div[@data-logger='news__MainTopNews']//a[contains(@class, *)]/@href"))

    posts = []

    for link in links:
        post = {}

        response = session.get(link, headers=header)
        dom = html.fromstring(response.text)

        title = dom.xpath(".//h1/text()")
        source = dom.xpath(".//div[contains(@class, 'breadcrumbs')]//span[@class='link__text']/text()")
        date = dom.xpath(".//@datetime")

        post['link'] = link
        post['date'] = date[0]
        post['source'] = source[0]
        post['title'] = title[0]
        post['_id'] = str(link)

        posts.append(post)

        try:
            news_collection.insert_one(post)
        except errors.DuplicateKeyError:
            print(f' {post["_id"]} Already exists!')


if __name__ == "__main__":
    mailru_news_parser()