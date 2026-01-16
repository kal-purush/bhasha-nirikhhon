from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep, strftime
from random import randint
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

usernames = "<Your-Username>"         
passwords = "<Your-Password>"


binary = "C:\\Program Files\\Mozilla Firefox\\firefox.exe"         #replace the path with your firefox.exe path
optionss = Options()
optionss.binary = binary
cap = DesiredCapabilities().FIREFOX.copy()
cap["marionette"] = True

webdriver = webdriver.Firefox(options=optionss, capabilities=cap, executable_path="F:\\Projects\\Instagram Bot\\geckodriver-v0.27.0-win64\\geckodriver.exe")   #replace the path to your geckodriver
sleep(2)

webdriver.get('https://www.instagram.com/accounts/login/?source=auth_switcher')
sleep(3)

username = webdriver.find_element_by_name('username')
username.send_keys(usernames)
password = webdriver.find_element_by_name('password')
password.send_keys(passwords)

button_login = webdriver.find_element_by_css_selector('.L3NKy > div:nth-child(1)')
button_login.click()
sleep(3)

hashtag_list = ['shrenikjangada']           #List of people You want to follow  or List of hashtags

for hashtag in hashtag_list:
    tag += 1
    #webdriver.get('https://www.instagram.com/explore/tags/'+ hashtag_list[tag] + '/')       #for following hashtags uncomment this and comment below line
    webdriver.get('https://www.instagram.com/'+ hashtag_list[tag] + '/')                  
    sleep(5)
    first_thumbnail = webdriver.find_element_by_xpath('/html/body/div[1]/section/main/div/header/section/div[1]/div[1]')
    first_thumbnail.click()
    sleep(randint(1,2))