from selenium import webdriver
from selenium.webdriver.common.by import By

firefox = webdriver.Firefox()
chrome = webdriver.Chrome()

try:
    chrome.get("https://the-internet.herokuapp.com/add_remove_elements/")
    firefox.get("https://the-internet.herokuapp.com/add_remove_elements/")

    for _ in range (5):
        add_button = chrome.find_element (
        By.XPATH, '//button [text ()="Add Element"]').click()
        add_button = firefox.find_element (
        By.XPATH, '//button [text ()="Add Element"]').click()

        chrome_delete_buttons = chrome.find_elements (
        "xpath", '//button [text ()="Delete"]')
        firefox_delete_buttons = chrome.find_elements (
        "xpath", '//button [text ()="Delete"]')
    
    print (
       f"список кнопок Delete в Chrome: {len(chrome_delete_buttons)}")
    print (
        f"список кнопок Delete в firefox: {len(firefox_delete_buttons)}")
    
except Exception as ex:
    print (ex)
finally:
    chrome.quit()
    firefox.quit()
