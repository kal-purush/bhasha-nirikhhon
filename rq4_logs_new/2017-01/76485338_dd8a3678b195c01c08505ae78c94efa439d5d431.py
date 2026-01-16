# -*- coding: utf-8 -*-
from selenium import webdriver


driver = webdriver.Chrome()
driver.get(
    'http://localhost/litecart/admin/?app=countries&doc=countries'
)
driver.find_element_by_name('username').send_keys(
    'admin'
)
driver.find_element_by_name('password').send_keys(
    'admin'
)
driver.find_element_by_name('login').click()
driver.find_element_by_css_selector('a.button').click()
main_window = driver.current_window_handle
ext_links = driver.find_elements_by_css_selector(
    'td>a[target=\'_blank\']'
)
for link in ext_links:
    link.click()
    current_windows = driver.window_handles
    driver.switch_to_window(current_windows[1])
    driver.close()
    driver.switch_to_window(main_window)
driver.quit()