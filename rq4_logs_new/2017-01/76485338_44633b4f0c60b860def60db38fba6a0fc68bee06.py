# -*- coding: utf-8 -*-
import pytest
from selenium import webdriver


@pytest.fixture(scope='session')
def wd(request):
    driver = webdriver.Chrome()
    driver.get('http://localhost/litecart/')
    request.addfinalizer(driver.quit)
    return driver


def test_reg_price_font_weight(wd):
    main_reg_price = wd.find_element_by_css_selector(
        'div#box-campaigns s.regular-price'
    )
    font_weight = main_reg_price.value_of_css_property(
        'font-weight'
    )
    assert font_weight == 'normal'


def test_reg_price_color(wd):
    main_reg_price = wd.find_element_by_css_selector(
        'div#box-campaigns s.regular-price'
    )
    color = main_reg_price.value_of_css_property('color')
    assert color == 'rgba(119, 119, 119, 1)'


def test_reg_price_decoration(wd):
    main_reg_price = wd.find_element_by_css_selector(
        'div#box-campaigns s.regular-price'
    )
    decoration = main_reg_price.value_of_css_property(
        'text-decoration'
    )
    assert decoration == 'line-through'


def test_camp_price_font_weight(wd):
    main_camp_price = wd.find_element_by_css_selector(
        'div#box-campaigns strong.campaign-price'
    )
    font_weight = main_camp_price.value_of_css_property(
        'font-weight'
    )
    assert font_weight == 'bold'


def test_camp_price_color(wd):
    main_camp_price = wd.find_element_by_css_selector(
        'div#box-campaigns strong.campaign-price'
    )
    color = main_camp_price.value_of_css_property('color')
    assert color == 'rgba(204, 0, 0, 1)'


def test_camp_price_decoration(wd):
    main_camp_price = wd.find_element_by_css_selector(
        'div#box-campaigns strong.campaign-price'
    )
    decoration = main_camp_price.value_of_css_property(
        'text-decoration'
    )
    assert decoration == 'none'


def test_matching(wd):
    main_product_name = wd.find_element_by_css_selector(
        'div#box-campaigns div.name'
    )
    main_name = main_product_name.get_attribute(
        'innerText'
    )
    main_regular_price = wd.find_element_by_css_selector(
        'div#box-campaigns s.regular-price'
    )
    main_reg_price = main_regular_price.get_attribute(
        'innerText'
    )
    main_campaign_price = wd.find_element_by_css_selector(
        'div#box-campaigns strong.campaign-price'
    )
    main_camp_price = main_campaign_price.get_attribute(
        'innerText'
    )
    product = wd.find_element_by_css_selector(
        'div#box-campaigns a.link'
    )
    product.click()
    details_product_name = wd.find_element_by_tag_name(
        'h1'
    )
    details_name = details_product_name.get_attribute(
        'innerText'
    )
    details_reg_price = wd.find_element_by_css_selector(
        's.regular-price'
    )
    reg_price = details_reg_price.get_attribute('innerText')
    details_camp_price = wd.find_element_by_css_selector(
        'strong.campaign-price'
    )
    camp_price = details_camp_price.get_attribute(
        'innerText'
    )
    assert main_name == details_name
    assert main_reg_price == reg_price
    assert main_camp_price == camp_price


def test_details_reg_price_font_weight(wd):
    details_reg_price = wd.find_element_by_css_selector(
        's.regular-price'
    )
    font_weight = details_reg_price.value_of_css_property(
        'font-weight'
    )
    assert font_weight == 'normal'


def test_details_reg_price_color(wd):
    details_reg_price = wd.find_element_by_css_selector(
        's.regular-price'
    )
    color = details_reg_price.value_of_css_property('color')
    assert color == 'rgba(102, 102, 102, 1)'


def test_details_reg_price_decoration(wd):
    details_reg_price = wd.find_element_by_css_selector(
        's.regular-price'
    )
    decoration = details_reg_price.value_of_css_property(
        'text-decoration'
    )
    assert decoration == 'line-through'


def test_details_camp_price_font_weight(wd):
    details_camp_price = wd.find_element_by_css_selector(
        'strong.campaign-price'
    )
    font_weight = details_camp_price.value_of_css_property(
        'font-weight'
    )
    assert font_weight == 'bold'


def test_details_camp_price_color(wd):
    details_camp_price = wd.find_element_by_css_selector(
        'strong.campaign-price'
    )
    color = details_camp_price.value_of_css_property(
        'color'
    )
    assert color == 'rgba(204, 0, 0, 1)'


def test_details_camp_price_decoration(wd):
    details_camp_price = wd.find_element_by_css_selector(
        'strong.campaign-price'
    )
    decoration = details_camp_price.value_of_css_property(
        'text-decoration'
    )
    assert decoration == 'none'