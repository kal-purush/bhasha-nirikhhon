# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CraigItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    bedrooms = scrapy.Field()
    bathrooms = scrapy.Field()
    town = scrapy.Field()
    description = scrapy.Field()
    link = scrapy.Field()
    