#-*-coding:utf-8-*-
#@Time      :2018/11/14  14:30
#@Author    ;河鱼不眠
#@Filename  :switch_ifram.py
#@Software  ;PyCharm
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

driver=webdriver.Chrome()
driver.get('http://www.baidu.com')
driver.maximize_window()
driver.find_element_by_id('kw').send_keys('腾讯课堂')
driver.find_element_by_id('su').click()

WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.XPATH,'//div[@id="1"]//a[@target="_blank"]')))
driver.find_element_by_xpath('//div[@id="1"]//a[@target="_blank"]').click()

window=driver.window_handles
driver.switch_to.window(window[-1])
current_window=driver.current_window_handle
WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.ID,'js_login')))
driver.find_element_by_id('js_login').click()
driver.implicitly_wait(2)
driver.find_element_by_class_name('btns-enter-qq').click()

driver.switch_to.frame('login_frame_qq')
driver.find_element_by_id('switcher_plogin').click()
driver.find_element_by_id('u').send_keys('361246837')
driver.find_element_by_id('p').send_keys('xqXQ361246!')
driver.find_element_by_id('login_button').click()

driver.quit()