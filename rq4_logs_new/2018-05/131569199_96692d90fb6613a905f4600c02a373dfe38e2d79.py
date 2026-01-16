from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
import time
import os

username = "bf39335099@sina.com"

password = "Aa11223344"

driver = webdriver.Firefox()
# options = Options()
# options.add_argument("--headless")
# driver = webdriver.Firefox(firefox_options=options)


driver.get("https://itunesconnect.apple.com/login")
driver.find_element_by_id("account_name_text_field").send_keys(username)
driver.find_element_by_class_name("icon icon_sign_in").click()
driver.find_element_by_id("password_text_field").send_keys(password)

