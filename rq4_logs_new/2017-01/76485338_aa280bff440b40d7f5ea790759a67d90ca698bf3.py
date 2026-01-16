# -*- coding: utf-8 -*-
from selenium import webdriver


driver = webdriver.Chrome()
driver.get('http://localhost/litecart/admin/')
driver.find_element_by_name('username').send_keys(
    'admin'
)
driver.find_element_by_name('password').send_keys(
    'admin'
)
driver.find_element_by_name('login').click()
links = driver.find_elements_by_css_selector(
        'li#app->a'
)
for i in xrange(len(links)):
    links = driver.find_elements_by_css_selector(
        'li#app->a'
    )
    links[i].click()
    sublinks = driver.find_elements_by_css_selector(
        'ul.docs a'
    )
    if len(sublinks) > 0:
        for s in xrange(len(sublinks)):
            sublinks = driver.find_elements_by_css_selector(
                'ul.docs a'
            )
            sublinks[s].click()
            assert len(
                driver.find_elements_by_tag_name('h1')
            ) == 1
    else:
        assert len(
            driver.find_elements_by_tag_name('h1')
        ) == 1
driver.quit()