from .base_page import BasePage
from selenium.webdriver.common.by import By
from .locators import ProductPageLocators

class ProductPage(BasePage):
	def should_be_button_add_to_basket(self):
		assert self.browser.is_element_present(*ProductPageLocators.BTN_BASKET), "Add to basket button is not presented"

	def add_product_to_basket(self):
		btn_basket = self.browser.find_element(*ProductPageLocators.BTN_BASKET)
		btn_basket.click()

	def should_be_name_product_matches_product_in_basket(self):
		product_name = self.browser.find_element(*ProductPageLocators.PRODUCT_NAME).text
		product_name_in_basket = self.browser.find_element(*ProductPageLocators.PRODUCT_IN_BASKET).text
		assert product_name == product_name_in_basket, "Product name does not match the product name in basket"

	def should_be_pirce_product_matches_price_basket(self):
		product_price = self.browser.find_element(*ProductPageLocators.PRODUCT_PRICE).text
		basket_price = self.browser.find_element(*ProductPageLocators.BASKET_PRICE).text
		assert product_price == basket_price, "Product price does not match the basket price"


