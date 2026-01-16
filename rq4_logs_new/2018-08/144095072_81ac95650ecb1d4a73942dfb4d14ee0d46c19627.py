# -*- coding: UTF-8 -*-
import re
import requests
from bs4 import BeautifulSoup

import time

import csv
import xlwt

import codecs
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class SpiderConfig(object):
    def __init__(self):
        self.config = {}
        
        self.config['targetnums'] = []

        self.config['url_base'] = 'http://www.weain.mil.cn/cgxq/yyzn/'
        self.config['headers'] = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'www.weain.mil.cn',
            'If-Modified-Since': 'Tue, 07 Aug 2018 16:49:12 GMT',
            'Upgrade-Insecure-Requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
        }

        self.config['filename'] = time.strftime("%Y%m%d%H%M%S", time.localtime())

    def set_config(self, key, val):
        if key and val and key in self.config.keys():
            self.config[key] = val


class WeainData(object):
    def __init__(self, writer):
        super(WeainData, self).__init__()
        self.writer = writer

        self.orderkeylist = ['name', 'num', 'type', 'field', 'function', 'index', 'url']

        self.data = {}
        self.data['name'] = ''
        self.data['num'] = ''
        self.data['type'] = ''
        self.data['field'] = ''
        self.data['function'] = ''
        self.data['index'] = ''
        self.data['url'] = ''

    def outputinfo(self):
        for key in self.orderkeylist:
            print '-',self.data[key]

    def fill(self, key, val):
        if key and val and key in self.data.keys():
            self.data[key] = val

    def write(self):
        data = []
        for key in self.orderkeylist:
            data.append(self.data[key])

        self.writer.writerow(data)

class WeainSpider(object):
    def __init__(self, spiderconfig):
        super(WeainSpider, self).__init__()
        self.spiderconfig = spiderconfig.config

        self.targeturls = [self.spiderconfig['url_base'] + str(num) + '.html'
         for num in self.spiderconfig['targetnums']]

        self.filename = self.spiderconfig['filename'] + '.csv'
        self.csvfile = open(self.filename, 'wb')
        self.csvfile.write(codecs.BOM_UTF8)
        self.writer = csv.writer(self.csvfile)

    def writetoexcel(self):
        workbook = xlwt.Workbook(encoding = 'utf-8')
        worksheet = workbook.add_sheet('data')
        excelfilename = self.spiderconfig['filename'] + '.xls'

        csvfile = open(self.filename, 'rb')
        reader = csv.reader(csvfile)

        worksheet.write(0,0,'项目名称')
        worksheet.write(0,1,'项目编号')
        worksheet.write(0,2,'项目类型')
        worksheet.write(0,3,'专业领域')
        worksheet.write(0,4,'功能用途')
        worksheet.write(0,5,'主要指标')
        worksheet.write(0,6,'URL')

        i = 1
        for item in reader:
            if not item[0]:
                continue
            for j in range(0,7):
                worksheet.write(i,j,item[j])
            i += 1

        workbook.save(excelfilename)
        csvfile.close()

    def crawl(self):
        i = 0
        for targeturl in self.targeturls:
            i += 1
            print i
            weaindata = WeainData(self.writer)

            try:
                response = requests.get(targeturl, headers=self.spiderconfig['headers'], timeout=10)
                time.sleep(1)

            except:
                print 'Fail to load:',targeturl

            else:
                page = response.content
                soup = BeautifulSoup(page, 'html.parser')
                
                try:
                    name = soup.select('h1')[0].string
                except:
                    name = '' 
                weaindata.fill('name', name)

                try:
                    num = soup.select('tr[class="even"] td')[1].string
                except:
                    num = ''
                weaindata.fill('num', num)

                try:
                    typ = soup.select('tr[class="even"] td')[3].string
                except:
                    typ = ''
                weaindata.fill('type', typ) 

                try:
                    field = soup.select('input[id="zyfx_yc"]')[0]['value']
                except:
                    field = ''
                weaindata.fill('field', field) 

                try:
                    fun = soup.select('div[class="view_box"] div[class="box"]')[0].string
                except:
                    fun = ''
                weaindata.fill('function', fun)

                try:
                    index = soup.select('div[class="view_box"] div[class="box"]')[1].string
                except:
                    index = ''
                weaindata.fill('index', index)

                url = targeturl
                weaindata.fill('url', url)

            weaindata.outputinfo()
            weaindata.write()

        self.csvfile.close()
        

if __name__ == '__main__':
    numfrom = 583235
    numto = 583556
    targetnums = [x for x in range(numfrom, numto + 1)]

    spiderconfig = SpiderConfig()
    spiderconfig.set_config('targetnums', targetnums)

    weainspider = WeainSpider(spiderconfig)
    weainspider.crawl()
    weainspider.writetoexcel()