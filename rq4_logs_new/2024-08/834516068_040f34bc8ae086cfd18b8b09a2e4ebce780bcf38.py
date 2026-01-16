from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import sleep

chrome = webdriver.Chrome()
wait = WebDriverWait(chrome, 40, 0.1)

try:
    chrome.get ("http://uitestingplayground.com/ajax")
    blue_button = chrome.find_element(By.CSS_SELECTOR, "#ajaxButton").click()
    text_from_content = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, ".bg-success"))).text
    print (text_from_content)

except Exception as ex:
    print(ex)
finally:
    chrome.quit() 