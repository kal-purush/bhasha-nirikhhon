from importlib import import_module
from requests import post
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import requests
import datetime
import time
import sys

# Callback Def
#
# callback(payload)
# payload: json string
# [
#   {
#       'type': 'TSMC',
#       'time': '2018-01-01',
#       'data': ["url"...]
#   },
#   {
#       'type': 'ASML',
#       'time': '2018-01-01',
#       'data': ["url"...]
#   },
#   ...
# ]

CRAWLER_ENDPOINT = sys.argv[1]
CRAWLER_PORT = sys.argv[2]

def submit_crawler(payload):
    p = {'payload': payload}
    post(f"http://{CRAWLER_ENDPOINT}:{CRAWLER_PORT}/crawler", json=p)

def cur_time_str():
    tz = datetime.timezone(datetime.timedelta(hours=8))
    return datetime.datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')

class UrlGenerator():
    def __init__(self):
        self.url = 'https://www.google.com/search?q='

    # Google search with queries and parameters
    def google_search(self, query, time='qdr:d', num=100):
        search_url = self.url + query + \
            '&tbm=nws&tbs=%s&num=%d&lr=lang_en' % (time, num)
        response = self.get_source(search_url)
        return self.parse_googleResults(response)

    def get_source(self, url):
        try:
            session = HTMLSession()
            response = session.get(url)
            return response
        except requests.exceptions.RequestException as e:
            print(e, file=sys.stderr)
            return None

    # Google Search Result Parsing
    def parse_googleResults(self, response):
        css_identifier_link = "WlydOe"
        css_identifier_results = "ftSUBd"
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.find("div", {"id": "search"}) is None:
            print('[%s] Currently banned from google search' % (cur_time_str()))
        results = soup.findAll("g-card", {"class": css_identifier_results})
        links = []
        for res in results:
          links.append(res.find("a", {"class": css_identifier_link})['href'])
        return links

    def generate_url(self, date, query, num=100):
        search_time = self.get_google_search_date(date)
        return self.google_search(query, time=search_time, num=num)

    def get_google_search_date(self, date):
        return 'cdr%3A1%2Ccd_min%3A{month}%2F{day}%2F{year}%2Ccd_max%3A{month}%2F{day}%2F{year}'.format(
            month=date.month, day=date.day, year=date.year
        )

if __name__ == '__main__':
    url_generator = UrlGenerator()

    # 1/25 ~ 6/7
    startdate = datetime.datetime(2022, 1, 25)

    for i in range(134):
        targetDate = startdate+datetime.timedelta(i)

        tsmc_urls = url_generator.generate_url(targetDate, "TSMC", num=10);
        asml_urls = url_generator.generate_url(targetDate, "ASML", num=10);
        am_urls = url_generator.generate_url(targetDate, "Applied Materials", num=10);
        sumco_urls = url_generator.generate_url(targetDate, "SUMCO", num=10);

        payload = [
            {
                'type': 'TSMC',
                'time': targetDate.strftime('%Y-%m-%d'),
                'data': tsmc_urls
            },
            {
                'type': 'ASML',
                'time': targetDate.strftime('%Y-%m-%d'),
                'data': asml_urls
            },
            {
                'type': 'Applied Materials',
                'time': targetDate.strftime('%Y-%m-%d'),
                'data': am_urls
            },
            {
                'type': 'SUMCO',
                'time': targetDate.strftime('%Y-%m-%d'),
                'data': sumco_urls
            }
        ]

        submit_crawler(payload)

        time.sleep(10)