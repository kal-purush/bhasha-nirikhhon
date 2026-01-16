#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/1/23 下午5:32
# @Author  : SageZhang
# @File    : DbmzSpider.py
# @Site    : www.sagezhang.com


from bs4 import BeautifulSoup
import requests

# --- 获取页码，默认1-10页，可更改1到第n页 ---
for i in range(1, 10):
    htmlpage = 'http://www.dbmeinv.com/dbgroup/show.htm?pager_offset=%d' % (i)
    # --- 定义headers ---
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:50.0) Gecko/20100101 Firefox/50.0'}
    req = requests.get(htmlpage, headers=headers)
    soup = BeautifulSoup(req.text, 'html.parser')
    link = soup.find_all('img')
    for a in link:
        src = a['src']
        print(src)
        name = src[-9:-4]  ##取URL以倒数第四至第九位第数字做图片的名字
        img = requests.get(src, headers=headers)
        f = open(name + '.jpg', 'ab')  ##写入多媒体文件必须要b这个参数
        f.write(img.content)  ##写入文件
        f.close()
        print(name + '已下载')
print("下载完成!")