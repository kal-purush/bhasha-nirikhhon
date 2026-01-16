# -*- coding:utf-8 -*-

import urllib2
import urllib
import sys
import re
import MySQLdb
from bs4 import BeautifulSoup

class DMZJ:
    def __init__(self):
        #动漫之家漫画分类页
        self.url = 'http://manhua.dmzj.com/tags/category_search/0-0-0-all-0-0-0-1.shtml'
        self.suburl = 'http://manhua.dmzj.com/tags/category_search/0-0-0-all-'
        self.suburl_ = '-0-0-1.shtml#category_nav_anchor'
        self.db = MySQLdb.connect('localhost','root','','cartoon',charset='utf8')
    
        
    #获取首页
    def getPage(self):
         request = urllib2.Request(self.url)
         response = urllib2.urlopen(request)
         return response.read().decode('utf-8')
        #r = requests.get(self.url)
    
    
    #爬取漫画类别链接并保存到数据库
    def getCategory(self):
        #链连接数据库
        db = self.db
        pattern = re.compile('<div class="search_list_m">(.*?)</div>', re.S)
        items = re.findall(pattern, self.getPage())
        htm_items = items[0]
        soup = BeautifulSoup(htm_items, "html.parser")
        span = soup.find_all('span', attrs={'class':'search_list_m_right'})
        for s in span:
            a = s.find_all('a',attrs={'class':'nav_item_type'})
            for a0 in a:
                print a0.get('id'),'**',a0.text
                typeurl = a0.get('id')
                typeurl = typeurl[5:len(typeurl)]
                typeurl = self.suburl+typeurl+self.suburl_
                print typeurl
                typename = a0.text
                #保存类别和链接
                sql = 'insert into ct_type(url,typename) values("%s","%s")' % (typeurl,typename)
                cursor = db.cursor()
                cursor.execute(sql) 
                db.commit()
        #关闭数据库
        db.close()        
                
                
dmzj = DMZJ()
dmzj.getCategory()
        