#!/usr/bin/env python
# encoding: utf-8


import requests
from bs4 import BeautifulSoup

def main():
    r = requests.get('https://movie.douban.com/tag/2016')
    soup = BeautifulSoup(r.text)
    article = soup.findAll('div', {'class': 'article'})[0]  # div for movies
    for table in article.findAll("table", {'class': 'infobox'}):
        table.extract()

    for div in article.findAll("div", {'class': ['clearfix', 'paginator']}):
        div.extract()

    # get all movie links
    for link in article.find_all('a', {'class': 'nbg'}):
        scrawl_movie_info(link.get('href'))
        # print(link.get('href'))


def scrawl_movie_info(url='https://movie.douban.com/subject/25980443/'):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    link = soup.select('#comments-section > div.mod-hd > h2 > span > a')[0].get('href')
    # print(link.get('href'))
    scrawl_movie_comments(link)


def scrawl_movie_comments(url):
    r = requests.get(url)
    # print(r.text)
    soup = BeautifulSoup(r.text, "html.parser")
    for item in soup.findAll('div', {'class': 'comment-item'}):
        for comment in item.findAll('p'):
            desc = comment.text  # get comment

        for rating in item.findAll('span', {'class': 'rating'}):
            score = rating['class'][0][7:]
            # print(score)

        print(score, desc)



main()
# scrawl_movie_comments('https://movie.douban.com/subject/25980443/comments?status=P')
