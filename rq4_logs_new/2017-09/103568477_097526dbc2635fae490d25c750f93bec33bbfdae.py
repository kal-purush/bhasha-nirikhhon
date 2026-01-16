import requests
from bs4 import BeautifulSoup
from lxml import html
from lxml import etree

user_id = 12345
url = 'http://www.uldosug.com/afisha'
r = requests.get(url)
with open('test.html', 'w') as output_file:
    output_file.write(r.text.encode('UTF-8'))


def read_file(filename):
    with open(filename) as input_file:
        text = input_file.read()
    return text


def parse_user_datafile_bs(filename):
    text = read_file(filename)

    tree = html.fromstring(text)
    list_blocs = tree.xpath('//div[@class = "aidanews2_positions"]')
    titles = list_blocs[0].xpath('//div[@class = "aidanews2_head"]')[0].xpath(
        '//h3[@class = "aidanews2_title"]/a/text()')
    # print etree.tostring(title)
    return titles



parse_user_datafile_bs('test.html')