# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PitchforkItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
	artist = scrapy.Field()
	album = scrapy.Field()
	label = scrapy.Field()
	year = scrapy.Field()
	score = scrapy.Field()
	reviewer = scrapy.Field()
	genre = scrapy.Field()
	review_text = scrapy.Field()
	review_date = scrapy.Field()
	best_new_music = scrapy.Field()
	best_new_reissue = scrapy.Field()