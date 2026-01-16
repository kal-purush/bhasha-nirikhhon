$ chmod +x ~/script/get_yahoo_weather.py
$ cat ~/script/get_yahoo_weather.py
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
 
import requests
from bs4 import BeautifulSoup
import sys
 
####################################
# 1〜9までの数値1つを引数にとるスクリプト
 
# 1:今日の天気
# 2:明日の天気
# 3:明後日の天気
# 4:今日の気温
# 5:明日の気温
# 6:明後日の気温
# 7:今日の降水確率
# 8:明日の降水確率
# 9:明後日の降水確率
####################################
 
arg = sys.argv
if len(arg) <= 1:
  url = "https://weather.yahoo.co.jp/weather/jp/13/"
  target = "引数がありません"
  flg = "7"
  print(target)
else:
  if arg[1] == "1":
    url = "https://weather.yahoo.co.jp/weather/jp/13/"
    target = "今日の天気"
    flg = "1"
  elif arg[1] == "2":
    url = "https://weather.yahoo.co.jp/weather/jp/13/?day=2"
    target = "明日の天気"
    flg = "2"
  elif arg[1] == "3":
    url = "https://weather.yahoo.co.jp/weather/jp/13/?day=3"
    target = "明後日の天気"
    flg = "3"
  elif arg[1] == "4":
    url = "https://weather.yahoo.co.jp/weather/jp/13/"
    target = "今日の気温"
    flg = "4"
  elif arg[1] == "5":
    url = "https://weather.yahoo.co.jp/weather/jp/13/?day=2"
    target = "明日の気温"
    flg = "5"
  elif arg[1] == "6":
    url = "https://weather.yahoo.co.jp/weather/jp/13/?day=3"
    target = "明後日の気温"
    flg = "6"
  elif arg[1] == "7":
    url = "https://weather.yahoo.co.jp/weather/jp/13/"
    target = "今日の降水確率"
    flg = "7"
  elif arg[1] == "8":
    url = "https://weather.yahoo.co.jp/weather/jp/13/?day=2"
    target = "明日の降水確率"
    flg = "8"
  elif arg[1] == "9":
    url = "https://weather.yahoo.co.jp/weather/jp/13/?day=3"
    target = "明後日の降水確率"
    flg = "9"
  else:
    url = "https://weather.yahoo.co.jp/weather/jp/13/"
    target = "引数が変です"
    flg = "10"
    print(target)
 
html = requests.get(url)
soup = BeautifulSoup(html.text, 'lxml')
 
anounce = soup.find('span', class_='time').text
place = soup.find('h1').text
 
weather = soup.find('dd', class_='forecast').p.img.get('alt')
high = soup.find('em', class_='high').get_text()
low = soup.find('em', class_='low').get_text()
precip = soup.find('p', class_='precip').get_text()
 
if flg != "10":
  phrase = anounce + " " + place + " " + target + 'は、'
 
if flg == "1" or flg == "2" or flg == "3":
   phrase += weather + 'です。' + " " + '最高気温は、' + high + "度" + " " + '最低気温は、' + low + '度、' + '降水確率は、' + precip + 'です。'
elif flg == "4" or flg == "5" or flg == "6":
   phrase += '最高気温' + " " + high + "度" + " " + '最低気温' + " " + low + '度です。'
elif flg == "7" or flg == "8" or flg == "9":
   phrase += precip + 'です。'
else:
  target
 
if flg != "10":
  print(phrase)
$