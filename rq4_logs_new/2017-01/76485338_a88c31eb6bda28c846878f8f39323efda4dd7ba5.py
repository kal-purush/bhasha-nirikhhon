# -*- coding: utf-8 -*-
import pytest

from selenium import webdriver
from selenium.webdriver.support.select import Select


@pytest.fixture(scope='session')
def driver(request):
    wd = webdriver.Chrome()
    wd.implicitly_wait(20)
    request.addfinalizer(wd.quit)
    return wd


@pytest.fixture(scope='session', autouse=True)
def page(driver):
    driver.get('http://localhost/litecart/admin/')
    driver.find_element_by_name('username').send_keys('admin')
    driver.find_element_by_name('password').send_keys('admin')
    driver.find_element_by_name('login').click()


def test_adding_product(driver):
    driver.find_element_by_css_selector(
        'a[href=\'http://localhost/litecart/admin/?app=catalog&doc=catalog\']'
    ).click()

    driver.find_element_by_css_selector(
        'a[href=\'http://localhost/litecart/admin/?category_id=0&app=catalog&doc=edit_product\']'
    ).click()

    driver.find_element_by_css_selector(
        'input[value=\'1\']'
    ).click()

    driver.find_element_by_name('name[en]').send_keys(
        'New product'
    )

    driver.find_element_by_name('code').send_keys(
        'New code'
    )

    driver.find_element_by_css_selector(
        'input[data-name=\'Rubber Ducks\']'
    ).click()

    driver.find_element_by_css_selector(
        'input[value=\'1-3\']'
    ).click()

    driver.find_element_by_name('quantity')

    driver.find_element_by_name('quantity').clear()

    driver.find_element_by_name('quantity').send_keys('10')

    Select(driver.find_element_by_name(
        'sold_out_status_id'
    )).select_by_value('')

    driver.find_element_by_css_selector(
        'input[type=\'file\']'
    ).send_keys(
        '/home/yury/PycharmProjects/selenium_course/adding_product/new_product.png'
    )

    driver.find_element_by_name(
        'date_valid_from'
    ).send_keys('01/14/2017')

    driver.find_element_by_name(
        'date_valid_to'
    ).send_keys('01/14/2018')

    driver.find_element_by_css_selector(
        'a[href=\'#tab-information\']'
    ).click()

    Select(
        driver.find_element_by_name('manufacturer_id')
    ).select_by_value('1')

    driver.find_element_by_name('keywords').send_keys(
        'key words'
    )

    driver.find_element_by_name(
        'short_description[en]'
    ).send_keys('New rubber duck')

    driver.find_element_by_class_name(
        'trumbowyg-editor'
    ).send_keys('Lorem ipsum')

    driver.find_element_by_name(
        'head_title[en]'
    ).send_keys('Head Title')

    driver.find_element_by_name(
        'meta_description[en]'
    ).send_keys('Meta')

    driver.find_element_by_css_selector(
        'a[href=\'#tab-prices\']'
    ).click()
    driver.find_element_by_name('purchase_price').clear()

    driver.find_element_by_name(
        'purchase_price'
    ).send_keys('5')

    Select(driver.find_element_by_name(
        'purchase_price_currency_code'
    )).select_by_value('USD')

    driver.find_element_by_name(
        'prices[USD]'
    ).send_keys('4')

    driver.find_element_by_name('save').click()

    assert len(driver.find_elements_by_xpath(
        '//a[text() = \'New product\']'
    )) == 1