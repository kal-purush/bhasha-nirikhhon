import scrapy
from scrapy.http import Request
from scrapy_sqlite3.items import ScrapySqlite3Item

def product_info(response, value):
    return response.xpath("//th[text()='"+value+"']/following-sibling::td/text()").get()

class BooksSpider(scrapy.Spider):
    name = 'books'
    allowed_domains = ['books.toscrape.com']
    start_urls = ['http://books.toscrape.com/']

    def parse(self, response):
        books_url = response.xpath("//h3/a/@href").getall()
        for book in books_url:
            absolute_url_book = response.urljoin(book)
            yield Request(absolute_url_book, callback=self.parse_book)

    def parse_book(self, response):
        items = ScrapySqlite3Item ()

        title =  response.xpath("//h1/text()").get()
        rating = response.xpath("//p[contains(@class, 'star-rating')]/@class").get()
        upc = product_info(response, 'UPC')
        product_type = product_info(response, 'Product Type')

        items['title'] = title
        items['rating'] = rating
        items['upc'] = upc 
        items['product_type'] = product_type 

        yield items