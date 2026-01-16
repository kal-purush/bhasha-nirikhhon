from selenium import webdriver
from selenium.webdriver.support.select import Select
from time import sleep
driver = webdriver.Chrome()
driver.get("http://stuinfo.neu.edu.cn/#/")

user=driver.find_element_by_xpath(r'//input[@type="text"]')
user.clear()
user.send_keys("账号")
user.click()
password = driver.find_element_by_xpath(r'//input[@type="password"]')
password.clear()
password.send_keys("密码")
password.click()
#http://stuinfo.neu.edu.cn/cloud-xxbl/studentinfo?tag=FKXXTJ:c012e3fc-ce6b-4cdc-9106-6c8595216de8
#http://stuinfo.neu.edu.cn/cloud-xxbl/studentinfo?tag=FKXXTJ:408d38cf-5949-444a-9577-8bd7685279d0
sign_in = driver.find_element_by_xpath(r'//button[@class="loginButton"]')
sign_in.click()
sleep(3)

server_center = driver.find_element_by_xpath(
    r'//*[@id="app"]/section/aside/div/ul/li[2]')
server_center.click()
sleep(1)

punch_in = driver.find_element_by_xpath(
    r'//*[@id="app"]/section/section/section/section/main/div[1]/div[1]/div/div[2]/div/div/div'
)
punch_in.click()
sleep(3)

n = driver.window_handles
driver.switch_to_window(n[-1])
print(driver.current_url)

sfgcyiqz = driver.find_element_by_id("sfgcyiqz")
Select(sfgcyiqz).select_by_value('否')
hjnznl=driver.find_element_by_xpath(r'//*[@id="studentinfo"]/div[34]/input')
hjnznl.clear()
hjnznl.send_keys("天津")
hjnznl.click()
qgnl=driver.find_element_by_xpath(r'//*[@id="studentinfo"]/div[35]/input')
qgnl.clear()
qgnl.send_keys("哪儿也没去")
qgnl.click()
sfqtdqlxs=driver.find_element_by_xpath(r'//*[@id="sfqtdqlxs"]')
Select(sfqtdqlxs).select_by_value('否')
sfjcgbr=driver.find_element_by_xpath(r'//*[@id="sfjcgbr"]')
Select(sfjcgbr).select_by_value('否')
sfjcglxsry=driver.find_element_by_xpath(r'//*[@id="sfjcglxsry"]')
Select(sfjcglxsry).select_by_value('否')
sfjcgysqzbr=driver.find_element_by_xpath(r'//*[@id="sfjcgysqzbr"]')
Select(sfjcgysqzbr).select_by_value('否')
sfjtcyjjfbqk=driver.find_element_by_xpath(r'//*[@id="sfjtcyjjfbqk"]')
Select(sfjtcyjjfbqk).select_by_value('否')
sfqgfrmz=driver.find_element_by_xpath(r'//*[@id="sfqgfrmz"]')
Select(sfqgfrmz).select_by_value('否')
sfygfr=driver.find_element_by_xpath(r'//*[@id="sfygfr"]')
Select(sfygfr).select_by_value('无')
sfyghxdbsy=driver.find_element_by_xpath(r'//*[@id="sfyghxdbsy"]')
Select(sfyghxdbsy).select_by_value('无')
sfygxhdbsy=driver.find_element_by_xpath(r'//*[@id="sfygxhdbsy"]')
Select(sfygxhdbsy).select_by_value('无')
sfbrtb=driver.find_element_by_xpath(r'//*[@id="sfbrtb"]')
Select(sfbrtb).select_by_value('是')

submit=driver.find_element_by_xpath(r'/html/body/div/div/div/button')
submit.click()

alert = driver.switch_to_alert()
sleep(2)
print (alert.text)
alert.accept()

driver.quit()