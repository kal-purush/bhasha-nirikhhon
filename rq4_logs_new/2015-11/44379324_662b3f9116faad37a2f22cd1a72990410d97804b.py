from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule

root_url = "http://nl.hardware.info/productgroep/{}/producten"
allowed_urls = (
    "7/behuizingen",
    "20/geheugenmodules",
    "1/moederborden",
    "3/processors",
    "21/voedingen",
    "5/videokaarten",
    "23/casefans",
    "19/cpu-koelers",
    "4/harddisksssds",
    "2/optischedrives"
)


class HardwareInfoSpider(CrawlSpider):
    name = "hardwareinfo"
    allowed_domains = "http://nl.hardware.info"
    CONCURRENT_ITEMS = 1000
    CONCURRENT_REQUESTS = 100
    CONCURRENT_REQUESTS_PER_DOMAIN = 1000
    start_urls = (root_url.format(allowed_url) for allowed_url in allowed_urls)

    rules = (
        Rule(LinkExtractor(restrict_xpaths=("//table[@id='productresulttable']"
                                            "/tbody"),
                           allow=("specificaties",)),
             follow=True, callback="parse_item"),
    )

    def parse_item(self, response):
        key_xpath = "tr/td[@position=1]"
        value_xpath = "tr/td[@position=2]"

        for sel in response.xpath("//div[@id='columnleft']"):
            product = {}
            for row in sel.xpath("table/tbody/"):
                product[row.xpath(key_xpath).extract()] = row.\
                    xpath(value_xpath).extract()
            yield product