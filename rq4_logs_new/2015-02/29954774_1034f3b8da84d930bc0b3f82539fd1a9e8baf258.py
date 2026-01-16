# FileName : weiboSpider.py
# -*- coding: utf-8 -*-

import cookielib
import urllib
import urllib2
import pyquery
import logging
import time
import random
import sys
import socket
from lxml import etree
import spider as Spider
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    stream=sys.stdout
                    )
class weiboSpider(Spider.spider):
    def __init__(self,username,password):
        Spider.spider.__init__(self,username,password)

    #get the users' weibo
    def getWeibo(self,userid):
        #weibo list
        weiboList=[]
        content=''
        preURL='http://weibo.cn'
        postURL='/u/'+userid
        expectedURLSeg='weibo.cn/u/'+userid
        while True:
           #HTTPError AND TIMEOUT , BlockedException may arise
           content=self.downloadHTML(preURL+postURL,expectedURLSeg)
           #extract data
           weiboList.extend(self.extractFollowing(content,userid))
           #extract next url
           postURL=self.extractFollowingUrl(content)
           if postURL == None:
              print 'Cant get postURL'
              break
        return weiboList

    def extractFollowingUrl(self,content):
        tree=etree.HTML(content)
        for ele in tree.xpath(u"//div[@class='pa' and @id='pagelist']"):
            if ele.xpath(u"form/div/a")[0].text==u'下页':
                return ele.xpath(u"form/div/a")[0].get("href")
        return None

    #ATTENTION: Here assume spider follows nobody
    def extractFollowing(self,contents,userid):
        pageList=[]
        tree=etree.HTML(contents)
        i=0
        for ele in tree.xpath(u"//div[@class='c' and @id]"):
            try:
                obj={}
                weiboid=ele.get("id").split('_')[1]
                print weiboid
                hrefs = ele.xpath(u"div/span[@class='cmt']/a")
                fowardingWeiboId=""
                atsList=""
                content=""
                if hrefs:
                    fowardingWeiboId=hrefs[0].text
                    contentList=ele.xpath(u"div[last()]/text()")
                    for contents in contentList:
                        content+=contents
                    ats=ele.xpath(u"div[last()]/a")[0]
                else :
                    contentList=ele.xpath(u"div/span[@class='ctt']/text()")
                    for contents in contentList:
                        content+=contents
                    ats=ele.xpath(u"div/span[@class='ctt']/a")
                for at in ats:
                    strat=at.get('href')
                    if strat.find('/n/')!=-1:
                        atsList+=at.text[1:]+"//"
                #print content
                likeNum=ele.xpath(u"div[last()]/a[last()-3]")[0].text
                commentNum=ele.xpath(u"div[last()]/a[last()-1]")[0].text
                fowardingNum=ele.xpath(u"div[last()]/a[last()-2]")[0].text
                info=ele.xpath(u"div[last()]/span[@class='ct']")[0].text.split(u'来自')
                timearray=info[0].split(' ')
                if(len(timearray)==2):
                    if(timearray[0]=="今天"):
                        timearray[0] = time.strftime('%m月%d日',time.localtime(time.time()))
                    time_sent = timearray[0]+' '+timearray[1]
                elif(len(timearray)==1):
                    minute_ago = int(timearray[0].split('分钟前')[0])
                    minute_now = int(time.strftime('%M',time.localtime(time.time())))
                    date_minute_now = datetime.datetime.fromtimestamp(minute_now)
                    date_time_sent = date_minute_now - datetime.timedelta(minutes=minute_ago)
                    print date_time_sent
                    time_sent = datetime.datetime.strftime(date_time_sent,'%m月%d日 %H时%M分')
                plantform=info[1]
                if type(content) == type(None):
                    logging.warning('Did not get '+weiboid+'content ')
                    content= " "
                #print time_sent
                obj['wid']=weiboid
                obj['uid']=userid
                obj['content']=content
                obj['time_sent']=time_sent
                obj['fowardingNum']=fowardingNum
                obj['commentNum']=commentNum
                obj['likeNum']=likeNum
                obj['plantform']=plantform
                obj['fowardingWeiboId']=fowardingWeiboId
                obj['atsList']=atsList
                pageList.append(obj)
                #pageList.append(weiboid+','+userid+','+content+','+time_sent+','+fowardingNum+','+commentNum +','+likeNum+','+plantform+','+fowardingWeiboId+','+atsList)
            except AttributeError as e:
                logging.warning(str(e))
                continue
        return pageList

if __name__ == '__main__':
    #!!!!USE YOUR USERNAME AND PASSWORD HERE
    Spider.spider = weiboSpider("18811442500", "742612")
    count=1
    file = open("test-uids")
    while 1:
        line = file.readline()
        if not line:
            break
        pass # do something
        print count
        uid=line[0:10]
        print uid
        count=count+1
        Spider.spider.getWeibo(uid)