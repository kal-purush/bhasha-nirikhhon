#-*- coding: UTF-8 -*-


import os
import requests
from bs4 import BeautifulSoup
import urllib
import re

url =  'https://accounts.douban.com/login'
data={
    'redir': 'https://www.douban.com/',
    'form_email':'857634746@qq.com',
    'form_password':'xuye18235108336',
    'login':'登录'
}
headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}
r = requests.post(url, data, headers=headers)
page = r.text

soup = BeautifulSoup(page,"html.parser")
captcha_url = soup.find('img',id='captcha_image')['src']
pattern = re.compile('<input type="hidden" name="captcha-id" value="(.*?)"/')
captcha_id = re.findall(pattern, page)

urllib.urlretrieve(captcha_url,"captcha.jpg")
os.system('/Users/yarving/.iterm2/imgcat captcha.jpg')
captcha = raw_input('please input the captcha:')
data['captcha-solution'] = captcha
data['captcha-id'] = captcha_id

r = requests.post(url, data=data, headers=headers)

page = r.text
soup = BeautifulSoup(page,"html.parser")
result = soup.findAll('div',attrs={'class':'title'})
# print result
for item in result:
    print item.find('a').get_text()