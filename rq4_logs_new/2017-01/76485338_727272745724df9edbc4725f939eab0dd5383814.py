# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.support.select import Select


driver = webdriver.Chrome()
driver.get('http://localhost/litecart/en/')

reg_link = driver.find_element_by_css_selector('td>a')
reg_link.click()

firstname_input = driver.find_element_by_name(
    'firstname'
)
firstname_input.send_keys('First Name')

lastname_input = driver.find_element_by_name(
    'lastname'
)
lastname_input.send_keys('Last Name')

address1_input = driver.find_element_by_name(
    'address1'
)
address1_input.send_keys('Address 404')

postcode_input = driver.find_element_by_name(
    'postcode'
)
postcode_input.send_keys('12345')

city_input = driver.find_element_by_name('city')
city_input.send_keys('City')

country_select = Select(
    driver.find_element_by_name('country_code')
)
country_select.select_by_value('US')

state_select = Select(
    driver.find_element_by_css_selector(
        'select[name=zone_code]'
    )
)
state_select.select_by_value('CA')

email_input = driver.find_element_by_name('email')
email_input.send_keys('test1@fake.mail')

phone_input = driver.find_element_by_name('phone')
phone_input.send_keys('123456789')

password_input = driver.find_element_by_name(
    'password'
)
password_input.send_keys('P@SsW0Rd')

confirm_pass_input = driver.find_element_by_name(
    'confirmed_password'
)
confirm_pass_input.send_keys('P@SsW0Rd')

create_button = driver.find_element_by_css_selector(
    'button[name=create_account]'
)
create_button.click()

logout_link = driver.find_element_by_css_selector(
    'a[href=\'http://localhost/litecart/en/logout\']'
)
logout_link.click()

email_login_input = driver.find_element_by_name(
    'email'
)
email_login_input.send_keys('test1@fake.mail')

password_login_input = driver.find_element_by_name(
    'password'
)
password_login_input.send_keys('P@SsW0Rd')

login_button = driver.find_element_by_name('login')
login_button.click()

logout_link = driver.find_element_by_css_selector(
    'a[href=\'http://localhost/litecart/en/logout\']'
)
logout_link.click()

driver.quit()