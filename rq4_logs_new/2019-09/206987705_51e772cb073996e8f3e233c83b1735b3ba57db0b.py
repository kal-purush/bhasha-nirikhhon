#!/usr/bin/env python3
# coding:utf-8

# ********** 需要安装模块 ************
# pip install selenium
# 下载PhantomJS http://phantomjs.org/download.html
# ************************************

from selenium import webdriver
from selenium.webdriver.common.by import By
import json
import time

# 双色球爬取
driver = webdriver.PhantomJS(executable_path='D:/Programs/phantomjs/bin/phantomjs')

if __name__=='__main__':
    urls = []
    items = []
    # 起始页
    start_url = "http://kaijiang.500.com/shtml/ssq/03001.shtml"
    driver.get(start_url)
    list = driver.find_elements_by_css_selector('body > div.wrap > div.kj_main01 > div.kj_main01_right > div.kjxq_box02 > div.kjxq_box02_title > div.kjxq_box02_title_right > span > div > a')
    for a in list:
        url = a.get_attribute("href");
        urls.append(url)
        print(url)
    print("urls加载完成。。。")

    i = 0
    for url in urls:
        try:
            print(url + "页面数据加载中。。。")
            i = i + 1
            # ('Connection aborted.', ConnectionResetError(10054, '远程主机强迫关闭了一个现有的连接。', None, 10054, None))
            # 每100条数据休眠10秒
            if i % 100 == 99:
                print("开始休眠")
                time.sleep(10)
                print("结束休眠")
            driver.get(url)
            # 第n期数据
            num = driver.find_element(By.XPATH, '//span[@class="span_left"]/a/font/strong').text
            # 开奖时间
            date = driver.find_element(By.XPATH, '//span[@class="span_right"]').text
            # bells
            bell_1 = driver.find_element(By.XPATH, '//li[@class="ball_red"][1]').text
            bell_2 = driver.find_element(By.XPATH, '//li[@class="ball_red"][2]').text
            bell_3 = driver.find_element(By.XPATH, '//li[@class="ball_red"][3]').text
            bell_4 = driver.find_element(By.XPATH, '//li[@class="ball_red"][4]').text
            bell_5 = driver.find_element(By.XPATH, '//li[@class="ball_red"][5]').text
            bell_6 = driver.find_element(By.XPATH, '//li[@class="ball_red"][6]').text
            bell_blue = driver.find_element(By.XPATH, '//li[@class="ball_blue"]').text
            item = {'num': num, 'date': date, 'bell_1': bell_1, 'bell_2': bell_2, 'bell_3': bell_3, 'bell_4': bell_4, 'bell_5': bell_5, 'bell_6': bell_6, 'bell_blue': bell_blue}
            items.append(str(item))
            print("第" + num + "期数据已加载。。。")
        except Exception as err:
            # 有些页面打不开，直接跳过
            print(url + "加载失败")
            pass
        continue

    # 写入数据
    with open('data.json', 'w', encoding='utf-8') as data:
        # ensure_ascii=False, 确保不会中文乱码
        # indent=4 美化json样式
        json.dump(items, data, ensure_ascii=False, indent=4)
