# -*- coding: utf-8 -*-
import pytest

from selenium import webdriver
from selenium.webdriver.support.select import Select


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


def test_countries_sorting(driver):
    driver.get(
        'http://localhost/litecart/admin/?app=countries&doc=countries'
    )
    unsorted_list = []
    rows = driver.find_elements_by_css_selector('tr.row')
    for row in rows:
        cells = row.find_elements_by_tag_name('td')
        unsorted_list.append(
            cells[4].get_attribute('textContent')
        )
    sorted_list = sorted(unsorted_list)
    assert sorted_list == unsorted_list


def test_zones_sorting(driver):
    rows = driver.find_elements_by_css_selector('tr.row')
    urls = []
    for row in rows:
        cells = row.find_elements_by_tag_name('td')
        if cells[5].get_attribute('textContent') != '0':
            link = cells[4].find_element_by_tag_name('a')
            urls.append(link.get_attribute('href'))
    for url in urls:
        driver.get(url)
        zones = driver.find_elements_by_css_selector(
            'table#table-zones td>input[name*="name"]'
        )
        unsorted_list = []
        for i in xrange(len(zones) - 1):
            unsorted_list.append(
                zones[i].get_attribute('value')
            )
        sorted_list = sorted(unsorted_list)
        assert sorted_list == unsorted_list
        driver.get(
            'http://localhost/litecart/admin/?app=countries&doc=countries'
        )


def test_geo_zones_sorting(driver):
    driver.get(
        'http://localhost/litecart/admin/?app=geo_zones&doc=geo_zones'
    )
    links = driver.find_elements_by_css_selector('tr.row a')
    urls = []
    for link in links:
        if link.get_attribute('title') == '':
            urls.append(link.get_attribute('href'))
    for url in urls:
        driver.get(url)
        unsorted_list = []
        zones = driver.find_elements_by_css_selector(
            'select[name*=\'zone_code\']'
        )
        for zone in zones:
            selected = Select(zone).first_selected_option
            unsorted_list.append(selected.get_attribute('textContent'))
        sorted_list = sorted(unsorted_list)
        assert sorted_list == unsorted_list
        driver.get(
            'http://localhost/litecart/admin/?app=geo_zones&doc=geo_zones'
        )