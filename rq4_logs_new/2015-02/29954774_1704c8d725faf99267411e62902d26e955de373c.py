#!/bin/python
# -*- coding: utf-8 -*-

import cookielib
import urllib
import urllib2
import pyquery
import logging
import time
import random
import sys
from spider import spider
from spider import BlockedException
from lxml import etree
import re

reload(sys)
sys.setdefaultencoding("utf-8")

logging.basicConfig(level=logging.DEBUG,  
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                    datefmt='%a, %d %b %Y %H:%M:%S',  
                    stream=sys.stdout  
                    ) 


class profileSpider(spider):
    def __init__(self,username,password):
        spider.__init__(self,username,password)        

    #get the users he follows
    def getProfile(self,userid):
        url='http://weibo.cn/u/'+userid   
        expectedURLSeg='weibo.cn/u/'+userid    
        content=''
        try:
            #HTTPError AND TIMEOUT , BlockedException may arise
            content=self.downloadHTML(url,expectedURLSeg)  
            #extract data
            userinfo=self.extractNumInfo(userid,content);
                
            url='http://weibo.cn/'+userid+'/info'
            expectedURLSeg='weibo.cn/'+userid+'/info'
            #HTTPError AND TIMEOUT , BlockedException may arise
            content=self.downloadHTML(url,expectedURLSeg)  
            #extract data
            userinfo=userinfo+self.extractInfo(userid,content);    
            return userinfo
        except socket.timeout as e:
            #logging
            logging.warning('TIME OUT, try again from '+url) 
        return userinfo
        
    
    def extractNumInfo(self,userid,content):
        html=pyquery.PyQuery(content)
        tree=etree.HTML(content)
        try:
            weiboNum=tree.xpath('/html/body/div[2]/div/span/text()')[0][3:-1]
            followingNum=tree.xpath('/html/body/div[2]/div/a[1]/text()')[0][3:-1]
            fansNum=tree.xpath('/html/body/div[2]/div/a[2]/text()')[0][3:-1]
            numInfo=userid+','+weiboNum+','+followingNum+','+fansNum
            return numInfo
        except AttributeError as e:
            print e 
            return ''

    #ATTENTION: Here assume spider follows nobody
    def extractInfo(self,userid,content):
        html=pyquery.PyQuery(content)
        tree=etree.HTML(content)
        level=tree.xpath('/html/body/div[3]/a[1]/text()')[0][:-1]
        vipLevel=tree.xpath('/html/body/div[3]/text()')[1][5:-2]
        if ('未开' in vipLevel): 
            vipLevel='null'
        name='null'
        location='null'
        gender='null'
        birthday='null'
        intro='null'
        identifyContent='null'
        label1='null'
        label2='null'
        label3='null'    
        infoArray=tree.xpath('/html/body/div[5]/text()')
        
        for ele in infoArray:
            label=ele[0:2]            
            if cmp(label,'昵称') ==0:    
                name=ele[3:]
                name.replace(',','.')
                name.replace('\n','.') 
            if cmp(label,'认证') ==0:    
                identifyContent=ele[5:]
                identifyContent.replace(',','.')
                identifyContent.replace('\n','.')   
            if cmp(label,'性别') ==0:    
                gender=ele[3:]  
                gender.replace(',','.')
                gender.replace('\n','.') 
            if cmp(label,'地区') ==0:    
                location=ele[3:]  
                location.replace(',','.')
                location.replace('\n','.') 
            if cmp(label,'生日') ==0:    
                birthday=ele[3:] 
                birthday.replace(',','.')
                birthday.replace('\n','.')  
            if cmp(label,'简介') ==0:    
                intro=ele[3:]  
                intro.replace(',','.')
                intro.replace('\n','.') 
            if cmp(label,'标签') ==0:    
                labelArray=tree.xpath('/html/body/div[5]/a/text()')
                count=1
                for ele1 in labelArray:
                    if count==1:
                        label1=ele1;
                        label1.replace(',','.')
                        label1.replace('\n','.') 
                    if count==2:
                        label2=ele1;
                        label2.replace(',','.')
                        label2.replace('\n','.') 
                    if count==3:
                        label3=ele1;
                        label3.replace(',','.')
                        label3.replace('\n','.') 
                    count=count+1
            
        info=','+level+','+vipLevel+','+name+','+gender+','+location+','+birthday+','+intro+','+identifyContent+','+label1+','+label2+','+label3
        return info

if __name__ == '__main__':
    #!!!!USE YOUR USERNAME AND PASSWORD HERE
    spider = profileSpider('18612986170','18612986170')
    print spider.getProfile('1195031270')