#!/bin/python
# -*- coding: utf-8 -*-

from Entity import *

class Profile(Entity):
    def __init__(self):
        Entity.__init__(self)
        #用户id
        self.uid='null'
        #微博数
        self.weiboNum='null'
        #关注数
        self.followingNum='null'
        #粉丝数
        self.fansNum='null'
        #微博等级
        self.level='null'  
        #会员等级
        self.vipLevel='null'
        #昵称
        self.name='null'
        #地址
        self.location='null'
        #性别
        self.gender='null'
        #生日
        self.birthday='null'
        #简介
        self.intro='null'
        #认证信息
        self.identifyContent='null'
        #标签
        self.label=[]
        #学习经历
        self.education=[]
        #工作经历
        self.jobs=[]
         