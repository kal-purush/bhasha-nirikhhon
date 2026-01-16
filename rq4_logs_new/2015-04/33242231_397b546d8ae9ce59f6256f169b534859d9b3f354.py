#-*- coding:utf-8 -*-
#!/usr/bin/env python

import urllib2, re
import sys, locale
from __builtin__ import isinstance


#系统缺省的编码
print u'系统缺省的编码: %s' % sys.getdefaultencoding()
#系统当前的编码
print u'当前系统的编码: (%s, %s)' % locale.getdefaultlocale()


            
#分析首页得到每个图片集的链接
url = 'http://desk.zol.com.cn/dongwu/'
'''
没有成功
#设置头文件，防止服务器禁止访问
header = {'Host':'desk.zol.com.cn',
          'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:26.0) Gecko/20100101 Firefox/26.0',
          'Accept': 'text/html, application/xhtml + xml, application/xml; q=0.9, */*;q=0.8',
          'Accept-Encoding':'gzip, deflate',
          'Connection':"keep-alive"}

#建立连接请求,这时服务器返回页面信息给con,是一个对象
req = urllib2.Request(url, headers=header)
con = urllib2.urlopen(req)

#调用con对象的read()方法,返回html页面
doc = con.read()
print doc

#关闭连接
con.close()
'''

#将打开的网页转码成unicode
m = urllib2.urlopen(url).read().decode('GBK')
print isinstance(m, unicode)
print m
print u'测试一下是否显示中文正常？'