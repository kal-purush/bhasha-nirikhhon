# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By


driver = webdriver.Chrome()
wait = WebDriverWait(driver, 10)
driver.get('http://localhost/litecart/en/')

for i in xrange(1, 3):
    products = driver.find_elements_by_css_selector(
        'a.link[title~=\'Duck\']'
    )
    products[i].click()
    driver.find_element_by_css_selector(
        'button[value=\'Add To Cart\']'
    ).click()
    wait.until(EC.text_to_be_present_in_element(
        (By.CLASS_NAME, 'quantity'), str(i)
    ))
    driver.get('http://localhost/litecart/en/')

driver.find_element_by_css_selector(
    'a.link[href=\'http://localhost/litecart/en/checkout\']'
).click()

items = driver.find_elements_by_css_selector('td.item')

for i in xrange(2):
    wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, 'button[value=\'Remove\']')
    ))
    driver.find_element_by_css_selector(
        'button[value=\'Remove\']'
    ).click()
    wait.until(EC.staleness_of(items[i]))

driver.quit()