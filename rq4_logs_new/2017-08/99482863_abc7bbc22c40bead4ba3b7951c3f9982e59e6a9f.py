# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#  -*- coding: utf-8 -*-
# !/usr/bin/python

import urllib2
import urllib
import cookielib
import re

auth_url = 'http://202.205.80.10'
home_url = 'http://202.205.80.10';
data={
    "DDDDD":raw_input("StudentID:\n"),
    "upass":raw_input("Password:\n"),
    "0MKKey":"login"
}
post_data=urllib.urlencode(data)
headers ={
    "Cache-Control":"max-age=0",
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding":"gzip, deflate",
    "Accept-Language":"zh-CN,zh;q=0.8,en;q=0.6",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36",
    "Content-Length":"45",
    "Content-Type":"application/x-www-form-urlencoded",
    "Origin":"http://202.205.80.154",
    "Upgrade-Insecure-Requests":"1",
    "Connection":"keep-alive",
    "Host":"202.205.80.10", 
    "Referer": "http://202.205.80.154/"
}
cookieJar=cookielib.CookieJar()
opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(cookieJar))
req=urllib2.Request(auth_url,post_data,headers)
result = opener.open(req)
result = opener.open(home_url)
print "success!"