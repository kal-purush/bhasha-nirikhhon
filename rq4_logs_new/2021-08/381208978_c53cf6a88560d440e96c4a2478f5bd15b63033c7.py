import requests
from bs4 import BeautifulSoup
import sys
import datetime
import re
import random

#yahooニュースのトップページのURL
URL = "https://www.yahoo.co.jp/"
rest = requests.get(URL)

#ページの読み込み
soup = BeautifulSoup(rest.text, "html.parser")

#見出しとURLの情報を取得
data_list = soup.find_all(href=re.compile("news.yahoo.co.jp/pickup"))

#見出しのURLをループで取得、リストに格納
headline_link_list = [data.attrs["href"] for data in data_list]

#yahooニュースの構造　top→要約→本文

#独自改変　すべてでなく、ランダムでニュースを取得（８パターン）
headline_link = headline_link_list[random.randrange(8)]

#見出しURLから、要約ページの内容取得, 解析できるようにする
summary = requests.get(headline_link)
summary_soup = BeautifulSoup(summary.text, "html.parser")

#aタグの中に”続きを読む”が含まれてるテキストを抽出
summary_soup_a = summary_soup.select("a:contains('続きを読む')")[0]

#aタグの中のhrefのテキストを抽出して本文のURLを取得
news_body_link = summary_soup_a.attrs["href"]

#ページ内容取得、解析できるようにする
news_body = requests.get(news_body_link)
news_soup = BeautifulSoup(news_body.text, "html.parser")

#本文のタイトルを表示
print(news_soup.title.text)

datail_text = news_soup.find(class_=re.compile("Direct"))

#本文を出力
#hasattrで指定のオブジェクトが特定の属性を持っているか確認
#detail_text.textがNoneだった場合は何も表示しない
print(datail_text.text if hasattr(datail_text, "text") else '', end="\n\n\n")