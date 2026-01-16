# from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup
import re

def crawlUrl(url):
    # request = urlopen(url)
    # print(request)
    # print(request.text)

    # content = requests.get('http://db.auto.sina.com.cn/list-0-1-0-0-0-0-0-0-9-0-1.html')
    content = requests.get(url)
    # print(content.content)
    # print(content.text)

    # soup = BeautifulSoup(content.text, 'lxml')
    soup = BeautifulSoup(content.text)
    print('-----------------')
    alist = soup.find_all('a')
    for i in alist:
        print(i)


def main():
    print("This is Web Page Crawl !")
    crawlUrl("https://www.baidu.com/")

if __name__ == '__main__':
    main()