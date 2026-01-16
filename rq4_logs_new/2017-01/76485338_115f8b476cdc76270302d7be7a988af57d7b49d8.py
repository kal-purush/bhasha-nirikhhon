# -*- coding: utf-8 -*-
from selenium import webdriver

from pages.main_page import MainPage
from pages.product_page import ProductPage
from pages.checkout_page import CheckoutPage


class Application:
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.main_page = MainPage(self.driver)
        self.product_page = ProductPage(self.driver)
        self.checkout_page = CheckoutPage(self.driver)

    def quit(self):
        self.driver.quit()

    def pickup_product(self):
        self.main_page.open()
        self.main_page.chose_product()

    def purchase_product(self):
        self.product_page.add_to_cart()
        self.product_page.go_to_checkout()

    def remove_invoice(self):
        self.checkout_page.remove_item()

    def remove_message(self):
        self.driver.find_element_by_tag_name('em')
        return self

    def is_invoice_removed(self):
        return self.remove_message.get_attribute(
            'textContent'
        ) == 'There are no items in your cart.'