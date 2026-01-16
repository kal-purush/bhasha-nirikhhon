
import PyRSS2Gen
import datetime
import argparse
import requests
from bs4 import BeautifulSoup
import re
import feedparser


parser = argparse.ArgumentParser()
parser.add_argument('-n', '--name', required=True, help='name of the whole feed')
parser.add_argument('-f', '--file', required=True)
parser.add_argument('-u', '--url', required=True)
parser.add_argument('-b', '--base', help='narrow the lookup to the contents of this item')
parser.add_argument('-c', '--criteria', required=True, help='criteria of picking the base items')
parser.add_argument('-d', '--description', help='description scheme of individual items')
parser.add_argument('-l', '--link', required=True, help='link scheme of individual items. Base item name is \'item\'')
parser.add_argument('-t', '--title', help='title scheme of individual items')
parser.add_argument('-q', '--limit', help='number of items to take. >0 = take items from beginning, otherwise from end')
parser.add_argument('-x', '--cookies', help='cookies to include with the request')
parser.add_argument('-o', '--overwrite', help='don\'t fetch existing feed', action="store_true")

if args.overwrite:
    items = []
    oldlinks = []
else:
    feed = feedparser.parse(args.file)
    items = [
        PyRSS2Gen.RSSItem(
            title=x.title,
            link=x.link,
            description=x.summary,
        )
        for x in feed.entries
    ]
    oldlinks = [x.link for x in items]

soup = BeautifulSoup(requests.get(args.url, cookies=eval(args.cookies)).text)
if args.base:
    soup = soup.find(**eval(args.base))
limit = args.limit if args.limit else -50
if limit > 0:
    newitems = soup(**eval(args.criteria))[:limit]
else:
    newitems = soup(**eval(args.criteria))[limit:]
for item in newitems:
    link = eval(args.link)
    if link not in oldlinks:
        items.append(
            PyRSS2Gen.RSSItem(
                title=eval(args.title) if args.title else item,
                link=link,
                description=eval(args.description) if args.description else ''
                )
        )
# print(newitems)
# exit()

rss = PyRSS2Gen.RSS2(
    title=args.name,
    link=args.url,
    description="RSS maker made by Dariush.",
    language='en-us',
    lastBuildDate=datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
    docs='http://blogs.law.harvard.edu/tech/rss',
    items=items[-100:],
)
f = open(args.file, encoding="utf-8", mode="w")
f.write(rss.to_xml("utf-8").replace('><', '>\n<'))