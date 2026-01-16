#! /usr/bin/env python
# -*- coding:utf-8 -*-

# pip install builtwith  识别网站所用技术
# pip install python-whois 寻找网站所有者

# 示例网站  http://example.webscraping.com

import builtwith
import whois
import urllib2
import re
import itertools
import urlparse

res = builtwith.parse('http://example.webscraping.com')
# print type(res),res  #<type 'dict'>  {u'javascript-frameworks': [u'jQuery', u'Modernizr', u'jQuery UI'],}

who = whois.whois('baidu.com')
# print type(who),who  #<class 'whois.parser.WhoisCom'>


def download(url, user_agent='wswp', num_retries=2): ##下载网页并返回其HTML, 用户代理
    print 'Downloading:', url
    headers = {'User-Agent': user_agent}
    request = urllib2.Request(url, headers=headers)
    try:
        html = urllib2.urlopen(request).read()
    except urllib2.URLError as e:
        print 'Download error:', e.reason
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500<=e.code<600:
                # recursively retry Sxx HTTP errors 
                return download(url, num_retries-1)
    return html

# download('http://httpstat.us/500')
#res =  download('http://www.baidu.com')
#print type(res) #<type 'str'>


def craw_sitemap(url):  ##抓取网站地图
    link_dict = dict()
    # download the s itemap file
    sitemap= download(url)
    # extract the sitemap links
    links = re.findall('<loc>(.*?)</loc>',sitemap)
    # download each link 
    for link in links:
        print "link:", link
        html = download(link)
        if html is not None:
            link_dict[link] = html
        else:
            pass
    return link_dict

#res = craw_sitemap('http://example.webscraping.com/sitemap.xml')
#print res 
    
##忽略页面别名， 只遍历ID来下载    
def download_by_id(url,start=1,end=10,max_errors = 5,user_agent='wswp',num_retries=2):
    # max_errors: maximum number of consecutive download errors allowed 
    num_errors = 0 # current number of consecutive(连续的) download errors 
    pages = dict()
    for page in range(start,end+1):
        url_ = url % page
        html = download(url_, user_agent, num_retries)
        if html is None:
            # received an error trying to down load this webpage
            num_errors+=1
            if num_errors==max_errors:
                # reached maximum number of 
                # consecutive errors so exit
                break
        else:
            # success - can scrape the result 
            num_errors = 0
            pages[page]=html
    return pages
    
#res = download_by_id('http://example.webscraping.com/view/-%d',1,2)
#print res


def link_crawler(seed_url, link_regex):
    """Crawl from the given seed URL following links matched by link regex"""
    craw_queue = [seed_url]
    # keep track which URL’s have seen before 
    seen = set(craw_queue)
    while craw_queue:
        url = craw_queue.pop()  #默认移除并返回最后一个元素
        html = download(url)
        # filter for links matching our regular expression
        print get_links(html)
        for link in get_links(html):
            # check if link matches expected regex 
            if re.match(link_regex, link):
                # form absolute link 
                link = urlparse.urljoin(seed_url, link) 
                # check if have a l ready seen thi s link
                if link not in seen:
                    seen.add(link)
                    craw_queue.append(link)


def get_links(html): #return relative link list
    """Return a list of links from html"""
    # a regular expression to extract all links from the webpage 
    webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
    # list of all links from the webpage
    return webpage_regex.findall(html)   


link_crawler('http://example.webscraping.com','/places/default/(index|view)')