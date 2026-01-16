from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Safari()
driver.get("http://www.python.org")
assert "Python" in driver.title
elem = driver.find_element_by_name("q")
elem.sned_keys("pycon")
elem.sned_keys(Keys.RETURN)
assert "No result found." not in driver.page_source
driver.close()