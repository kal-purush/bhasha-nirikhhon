#!/bin/python
# -*- coding: utf-8 -*-


import logging
import sys
import socket

from lxml import etree
import re
import pyquery
import time,datetime
import HTMLParser

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from spider import spider
from spider import BlockedException



logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    stream=sys.stdout
                    )




class commentSpider(spider):

    def __init__(self,username,password):
        spider.__init__(self,username,password)

    # get weibo comment
    def getComment(self, userid,weiboid):
        #list to store the people he follows
        self.userid=userid
        self.weiboid=weiboid
        commentList = []

        preURL = 'http://weibo.cn/'
        postURL = 'comment/'+weiboid + '?uid=' + userid + '&rl=2&page=1'
        expectedURLSeg='comment/'+weiboid + '?uid=' + userid

        while True:
            #HTTPError AND TIMEOUT , BlockedException may arise
            content=self.downloadHTML(preURL+postURL,expectedURLSeg)
            #extract data
            tree=etree.HTML(content)
            commentList.extend(self.extractComment(tree))
            #extract next url
            postURL=self.extractFollowUrl(content)
            if postURL == None:
                break

        return commentList



    def extractFollowUrl(self,content):
        html=pyquery.PyQuery(content)
        for ele in html('form a'):
            if pyquery.PyQuery(ele).text()==u'下页':
                return pyquery.PyQuery(ele).attr('href')
        return None


    def extractComment(self, tree):
        commentList = []
        idList = tree.xpath('//div/@id')

        for id in idList:
            if(re.match('C_',id)):
                xpath_comment='//div[@id="'+id+'"]'+'/span[@class="ctt"]'
                xpath_comment_text='//div[@id="'+id+'"]'+'/span[@class="ctt"]/text()'
                xpath_comment_at = '//div[@id="'+id+'"]'+'/span[@class="ctt"]/a/text()'
                xpath_time='//div[@id="'+id+'"]'+'/span[@class="ct"]/text()'
                xpath_agree='//div[@id="'+id+'"]'+'/span[@class="cc"][1]/a/text()'
                #comment 评论内容
                comment = tree.xpath('string('+xpath_comment+')')
                #agree 赞同数
                agree=tree.xpath(xpath_agree)[0][2]
                timearray=tree.xpath(xpath_time)[0].encode('utf-8').split('来自')[0].split(' ')

                time_sent = self.timeFormat(timearray)

                print agree,time_sent,comment
                commentContent = {'userid':self.userid,'weiboid':self.weiboid,'comment':comment,'agree':agree,'time':time_sent}
                commentList.append(commentContent)

        return commentList
    #统一时间格式
    def timeFormat(self,timearray):
        # 今天
        if(len(timearray)==2):
            if(timearray[0]=="今天"):
                timearray[0] = time.strftime('%m月%d日',time.localtime(time.time()))
            time_sent = timearray[0]+' '+timearray[1]
        # xx分钟前
        elif(len(timearray)==1):
            minute_ago = int(timearray[0].split('分钟前')[0])
            minute_now = int(time.strftime('%M',time.localtime(time.time())))
            date_minute_now = datetime.datetime.fromtimestamp(minute_now)
            date_time_sent = date_minute_now - datetime.timedelta(minutes=minute_ago)
            print date_time_sent
            time_sent = datetime.datetime.strftime(date_time_sent,'%m月%d日 %H时%M分')
        return unicode(time_sent,'utf8')

if __name__ == '__main__':
    #!!!!USE YOUR USERNAME AND PASSWORD HERE
    spider = commentSpider(USERNAME, PASSWORD)
    print spider.getComment('1644088832','C1JhOhrwM')