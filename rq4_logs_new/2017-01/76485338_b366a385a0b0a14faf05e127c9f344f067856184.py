# -*- coding: utf-8 -*-
import pytest

from selenium import webdriver


@pytest.fixture(scope='session')
def driver(request):
    wd = webdriver.Chrome()
    request.addfinalizer(wd.quit)
    return wd


@pytest.fixture(scope='session', autouse=True)
def page(driver):
    driver.get('http://localhost/litecart/admin/')
    driver.find_element_by_name('username').send_keys(
        'admin'
    )
    driver.find_element_by_name('password').send_keys(
        'admin'
    )
    driver.find_element_by_name('login').click()


def test_logs(driver):
    driver.get(
        'http://localhost/litecart/admin/?app=catalog&doc=catalog&category_id=1'
    )
    links = driver.find_elements_by_css_selector(
        'td:nth-child(3)>a[href*=\'product_id\']'
    )
    urls = []
    for link in links:
        urls.append(link.get_attribute('href'))
    for url in urls:
        logs = driver.get_log('browser')
        driver.get(url)
        assert len(logs) == 0