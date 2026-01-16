# -*- coding: utf-8 -*-
import urllib2
import requests
import json

while True:
    cityname = raw_input('你想查哪个城市的天气？\n')


    url = 'http://wthrcdn.etouch.cn/weather_mini?city=%s' % cityname
    # content = urllib2.urlopen(url).read()
    # print content
    # data = json.loads(content)
    # print data
    content = requests.get(url)
    data = content.json()
    result = data.get('data')
    if result:
        # 当天情况
        temp = result.get('wendu')
        Aqi = result.get('aqi')
        ganmaochengdu = result.get('ganmao')
        print temp,Aqi,ganmaochengdu
        #预报
        print '后几天预报:'
        for f in result.get('forecast'):
            date = f.get('date')
            type = f.get('type')
            low_temp = f.get('low')
            high_temp = f.get('high')
            fengxiang = f.get('fengxiang')
            fengli = f.get('fengli')
            print  date,type,low_temp,high_temp,fengxiang,fengli
    else:
        print '查无此城市！'
        break
