from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Firefox()

driver_get = driver.get("http://www.jana.com")


assert "Jana" in driver.title


elem = driver.find_element_by_tag_name("span")
elem.send_keys(Keys.RETURN)
