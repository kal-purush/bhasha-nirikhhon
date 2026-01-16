# -*- coding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
import urllib
from lxml import etree
import re

def getHtml(url):
    page = urllib.urlopen(url)
    html = page.read().decode('utf-8')
    return html

def spider(url):
    get_div=getHtml(url)
    html = etree.HTML(getHtml(url))
    title = html.xpath('//h1[@align="center"]')
    t = html.xpath('//div[@align="center"]')
    time = t[0].text[:-4]
    f = open('/home/hc/PycharmProjects/dlut/xszz.txt', 'a')
    f.write(title[0].text)
    f.write("           ")
    f.write(time)
    f.write('\n')
    f.close()
    item=re.findall('<div id="vsb_content.*?">(.*?)<div id="div_vote_id"></div>', get_div, re.S)
    result = '<div id="vsb_content">'+etree.tostring(etree.HTML(item[0]))
    div = etree.HTML(result)
    get_son(div)

def get_son(father):
    father_xpath = etree.HTML(etree.tostring(father))
    son=father_xpath.xpath('//body/*/*')
    item = father_xpath.xpath('//body/*/text()')
    if len(son) == 0:
        return False
    for i in range(len(son)):
        if son[i].text != None:
            f = open('/home/hc/PycharmProjects/dlut/xszz.txt', 'a')
            f.write(son[i].text)
            f.close()
        if get_son(son[i]):
            print
        if son[i].tag=='p':
            f = open('/home/hc/PycharmProjects/dlut/xszz.txt', 'a')
            f.write('\n')
            f.close()
    if len(item) > 1:
        for i in range(len(item)-1):
            if item[i+1].strip()!='':
                f = open('/home/hc/PycharmProjects/dlut/xszz.txt', 'a')
                f.write('**补充:')
                f.write(item[i + 1])
                f.write('##')
                f.close()