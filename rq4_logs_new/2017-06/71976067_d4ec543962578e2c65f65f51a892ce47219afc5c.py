# -*- coding: utf-8 -*-
#from urllib import request
#import urllib.request
#from urllib import parse
import requests
import random
from redis_slave_connect import conn
import aiohttp

'''
class HtmlDownloader(object):

    def download(self, url):
        if url is None:
            return 

        req = request.Request(url)  # the html is downloaded in the local
        #req = Request(url)
        #携带浏览器的头部，防止网站反爬
        req.add_header("User-Agent","Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0")
        response = request.urlopen(req)
        if response.getcode() != 200:
            return None
        #print (response.read())
        return response.read()
'''
User_Agent = ['Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19',
'Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
'Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
'Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0',
'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36',
'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19',
'Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3',
'Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3']

class HtmlDownloader(object):
    async def download(self, page_url):
        #url = conn.brpop('task_url')
        url = page_url
        if url is None:
            return
        #useragent = random.choice(User_Agent)
        r = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0"
        headers = {'user-agent': r}
        #proxyip = conn.srandmember('proxyip_pool')
        #proxies = {'https': proxyip}
        url = url.decode('utf-8')
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                #assert resp.status == 200
                data = await resp.text(encoding = 'gbk')
                print(data)
                return data
        '''
        async with aiohttp.request('GET', url) as resp:
            assert resp.status == 200
            await resp.text(encoding = 'gbk')
            return resp.text

        '''
        '''使用requests同步请求
        try:
            #response = requests.get(url, proxies = proxies, timeout = 5)
            response = requests.get(url, headers = headers, timeout = 5)
            response.encoding = 'utf-8'
            print(response.status_code)
            print(response.text)
            return response.text
        except:
            print('request error')
        '''
'''
if __name__ == '__main__':
    test = HtmlDownloader()
    test.download('http://www.baidu.com')
'''