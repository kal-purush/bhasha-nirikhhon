#coding:utf-8
from selenium import webdriver
import time
driver = webdriver.Chrome()
driver.get('http://weixin.sogou.com/')
driver.find_element_by_id("upquery").send_keys(u"浙江旅游")
driver.find_element_by_class_name("swz2").click()
now_handle = driver.current_window_handle
driver.find_element_by_xpath('/html/body/div[2]/div[3]/div[1]/div[1]/div/div[2]/div/div[1]').click()
time.sleep(10)
all_handle = driver.window_handles
for handle in all_handle:
	print(handle)
	if handle != now_handle:
		driver.switch_to_window(handle)
		print u"当前网页title" + driver.title
#driver.quit()