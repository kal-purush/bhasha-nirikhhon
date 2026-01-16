# -*- coding: UTF-8 -*-
import http.client as httplib
# import time
import hashlib
from urllib import request
import random
import json
"""
1、安装库 pip install baidu-aip
2、直接运行
翻译app
"""
from aip import AipOcr
def ocrcat(a):
    result=''
    APPID='21876639'
    APIKEY='iqjr21CFPRXhBtWtul5EjMKQ'
    SECRETKEY='ZNYl7ZxXFMl4XyWu4LIse6i7EyGAnBB0'
    c=AipOcr(APPID,APIKEY,SECRETKEY)
    img=open(a,'rb').read()
    message=c.basicGeneral(img)
    file = a.split('.')[0]
    fo = open(file + ".txt", "w+")
    for item in message.get('words_result'):
        fo.write(item['words'])
        fo.write('\n')
        print(item['words'])

    fo.close()


#翻译模块

def translate(text,fromLang = 'auto',toLang = 'zh'):
    appid = '20200809000538167'
    secretKey = '0nhHOv8YjqmOtazfuoTD'

    httpClient = None
    myurl = '/api/trans/vip/translate'
    # fromLang = 'cht'#繁体
    # fromLang = 'auto'#自动检测
    # toLang = 'zh'
    salt = random.randint(32768, 65536)

    sign = appid + text + str(salt) + secretKey
    m1 = hashlib.md5()
    m1.update(sign.encode("utf8"))
    sign = m1.hexdigest()

    myurl = myurl + '?appid=' + appid + '&q=' + request.quote(text) + \
            '&from=' + fromLang + '&to=' + toLang + '&salt=' + str(
        salt) + '&sign=' + sign

    try:
        httpClient = httplib.HTTPConnection('api.fanyi.baidu.com')
        httpClient.request('GET', myurl)

        # response是HTTPResponse对象
        response = httpClient.getresponse()
        response_text = response.read()
        # print response_text
        response_json = json.loads(response_text)
        re_text = response_json['trans_result'][0]['dst']
        return re_text
    except Exception as e:
        print( "error",e)
    finally:
        if httpClient:
            httpClient.close()

if __name__ == '__main__':
    text = '新北市十二年國民基本教育資訊網'
    #text = 'クローラ'
    print(translate(text,'cht','en'))
#     time.sleep(1)