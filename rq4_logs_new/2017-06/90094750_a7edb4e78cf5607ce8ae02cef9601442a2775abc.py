from behave import given, when, then, use_step_matcher
from hamcrest import assert_that, equal_to
import re
from login_utils import *
from behave import *

@when(u'she logs in and clicks My Lists')
def see_add(context):
    add_found = context.browser.find_element_by_id("My lists")
    add_found.click()
    assert add_found


@then(u'she would see a add button and click it')
def see(context):
    add_found = context.browser.find_element_by_id('add')
    add_found.click()


@When(u'a user visits the new watchlist page')
def enter_add_new(context):
    my_lists = context.browser.find_element_by_id("My lists")
    my_lists.click()
    add = context.browser.find_element_by_id("add")
    add.click()

@then(u'she would enter the new watchlist page')
def enter_addpage(context):
    pass


@given (u'a user visits the new watchlist page')
def add_page(context):
    pass


@given(u'she should see the title of New Watchlist')
def newWatchlist_found(context):
    new_found = re.search("New Watchlist", context.browser.page_source, re.IGNORECASE)
    assert new_found


@then(u'she should see the cryptocurrency')
def currency_visit(context):
    currency_found = re.search("cryptocurrency", context.browser.page_source, re.IGNORECASE)
    assert currency_found

@then(u'she should see the Currency')
def currency_visit(context):
    currency_found = re.search("Currency", context.browser.page_source, re.IGNORECASE)
    assert currency_found


@then(u'she choose to add bitcoin and EUR as a new watchlist')
def choose_currency(context):
    cryptocurrency = context.browser.find_element_by_id('0')
    cryptocurrency.click()
    choose_cryptocurrency = context.browser.find_element_by_id('bitcoin')
    choose_cryptocurrency.click()
    currency = context.browser.find_element_by_id('1')
    currency.click()
    choose_currency = context.browser.find_element_by_id('EUR')
    choose_currency.click()
    confirm_button = context.browser.find_element_by_id('confirm')
    confirm_button.click()

@then(u'she choose to add ripple and EUR as a new watchlist')
def choose_currency(context):
    cryptocurrency = context.browser.find_element_by_id('0')
    cryptocurrency.click()
    choose_cryptocurrency = context.browser.find_element_by_id('ripple')
    choose_cryptocurrency.click()
    currency = context.browser.find_element_by_id('1')
    currency.click()
    choose_currency = context.browser.find_element_by_id('EUR')
    choose_currency.click()
    confirm_button = context.browser.find_element_by_id('confirm')
    confirm_button.click()

@then(u'she returns to the dashboard and see New watchlist added')
def seenew_watchlist(context):
    currency_found = re.search("New watchlist added", context.browser.page_source, re.IGNORECASE)
    assert currency_found

@then(u'she would see the information at bottom of the table')
def seenew_watchlist(context):
    currency_found = re.search("ripple", context.browser.page_source, re.IGNORECASE)
    assert currency_found