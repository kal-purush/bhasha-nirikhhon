from scrapy.selector import HtmlXPathSelector
from scrapy.spider import BaseSpider
from scrapy.http import Request

import scrapy

from tutorial.items import DmozItem

DOMAIN = "dmoz.org"
URL = "http://www.dmoz.org/Computers/Programming/Languages"

class DmozSpider(BaseSpider):
    name = "dmoz2"
    allowed_domains = [DOMAIN]
    start_urls = [URL]

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        for url in hxs.select('//a/@href').extract():
            if not url.startswith('http://'):
                url= URL + url 
            if 'http://www.dmoz.org/Computers/Programming/Languages' in url :
                with open('urls.txt', 'a') as f:
                  f.write('{0}\n'.format(url))