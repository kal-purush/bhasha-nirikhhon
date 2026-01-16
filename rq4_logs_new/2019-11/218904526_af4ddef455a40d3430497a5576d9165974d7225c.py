#!/usr/bin/env python
# -*- coding:utf-8 -*-

# from calibre.web.feeds.recipes import BasicNewsRecipe
import datetime
from bs4 import BeautifulSoup
from lib.urlopener import URLOpener
from lib.autodecoder import AutoDecoder
from base import BaseFeedBook, string_of_tag

__author__ = 'minxia'


def getBook():
    return BBCLearningEnglish


class BBCLearningEnglish(BaseFeedBook):
    title = u'BBC Learning English'
    description = u'BBC Learning English'
    language = 'en'  # TED中英文双语，为en则能英文查词
    feed_encoding = "utf-8"
    page_encoding = "utf-8"
    network_timeout = 60
    oldest_article = 1
    # 设置为True排版也很好（往往能更好的剔除不相关内容），
    # 除了缺少标题下的第一幅图
    fulltext_by_readability = False
    # keep_only_tags = [dict(name='div', attrs={'id': 'article'}), ]
    keep_only_tags = [{'class': 'widget-container widget-container-left'}]
    remove_tags = [{'class': ['widget widget-list widget-list-automatic',
                              'widget widget-pagelink widget-pagelink-next-activity ']}]

    feeds = [
        ("take-away-english", "http://www.bbc.co.uk/learningenglish/chinese/features/take-away-english"),
        ("the-english-we-speak", "http://www.bbc.co.uk/learningenglish/english/features/the-english-we-speak"),
        ("6-minute-english", "http://www.bbc.co.uk/learningenglish/english/features/6-minute-english"),
        ("lingohack","http://www.bbc.co.uk/learningenglish/english/features/lingohack")
    ]

    url_prefix = 'http://www.bbc.co.uk'

    def ParseFeedUrls(self):
        """ return list like [(section,title,url,desc),..] """
        urls = []
        i = 0
        for feed in self.feeds:
            feedtitle, url = feed[0], feed[1]
            opener = URLOpener(self.host, timeout=self.timeout)
            result = opener.open(url)
            if result.status_code != 200 or not result.content:
                self.log.warn('fetch webpage failed(%d):%s.' % (result.status_code, url))
                continue

            if self.feed_encoding:
                try:
                    content = result.content.decode(self.feed_encoding)
                except UnicodeDecodeError:
                    content = AutoDecoder(False).decode(result.content, opener.realurl, result.headers)
            else:
                content = AutoDecoder(False).decode(result.content, opener.realurl, result.headers)

            soup = BeautifulSoup(content, 'lxml')
            for article in soup.findAll('div', {"class": "text"}):
                if article.find("h2") and article.find("a"):
                    title = article.a.contents[0].strip()
                    if not title:
                        continue
                    href = self.url_prefix + article.a['href']

                    urls.append((feedtitle, title, href, None))
                    if i > 3:
                        break
                    else:
                        i = i + 1

        return urls