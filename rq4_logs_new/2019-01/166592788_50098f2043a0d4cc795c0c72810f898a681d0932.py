import scrapy

from masters.data_structures.Location import Location
from masters.utils import unicode_utils


class LocationsSpider(scrapy.Spider):
    name = "locations"
    root_url = 'https://www.tripadvisor.com'
    current_review_coordinates = ""
    urls = [
        '/Attractions-g644300-Activities-Kranj_Upper_Carniola_Region.html#FILTERED_LIST',
    ]

    def request(self, url, callback):
        request_with_cookies = scrapy.Request(
            url=(self.root_url + url),
            callback=callback)
        request_with_cookies.cookies['TALanguage'] = 'ALL'
        return request_with_cookies

    def start_requests(self):
        for url in self.urls:
            yield self.request(url, self.parse)

    def parse(self, response):
        next_href = response.css('div.pagination a.next::attr(href)').extract_first()
        if next_href is not None:
            next_review_page_url = unicode_utils.unicode_to_string(next_href)
        else:
            next_review_page_url = ""
        location_group_name = unicode_utils.unicode_to_string(
            response.css('div.ui_container h1::text').extract_first()).replace("\n", "")
        location_current_page = unicode_utils.unicode_to_string(
            response.css('div.pageNumbers span.current::text').extract_first())

        locations = []
        for location in response.css('div.attraction_clarity_cell'):
            location_url = unicode_utils.unicode_to_string(
                location.css('div.listing_title a::attr(href)').extract_first())
            location_name = unicode_utils.unicode_to_string(
                location.css('div.listing_title a::text').extract_first())

            location_data = Location(location_name, location_url)
            locations.append(location_data)

        filename = 'data/data_locations/locations-%s-%s.csv' % (location_group_name, location_current_page)
        with open(filename, 'wb') as f:
            for location in locations:
                f.write(location.get_csv_line())
        self.log('Saved file %s' % filename)
        if next_review_page_url is not "":
            yield self.request(next_review_page_url, self.parse)