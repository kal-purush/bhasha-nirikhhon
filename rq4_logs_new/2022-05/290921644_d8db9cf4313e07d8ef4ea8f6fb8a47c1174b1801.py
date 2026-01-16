from helper_functions import *
from selenium.webdriver.common.by import By

class PortalOpener:
	def __init__(self, driver, username, password):
		self.driver = driver
		self.username = username
		self.password = password
		self.helper = HelperFunctions(self.driver)

	"""Open employee login page."""
	def to_employee_site(self):
		print('*** employee_site ***')
		portal_id = 'eco-primary-alternate'
		login_elem = self.driver.find_element_by_class_name(portal_id) # Element layout updated on website
		login_elem.click()

		self.driver.close()  # Login opens new tab, close and switch
		self.driver.switch_to.window( self.driver.window_handles[0] )
		self.driver.minimize_window()

	"""Input credentials."""
	def login(self):
		print('*** ess_login ***')
		self.helper.wait_for('#username', By.CSS_SELECTOR)
		user_elem = self.driver.find_element_by_css_selector('#username')
		user_elem.send_keys(self.username)
		pass_elem = self.driver.find_element_by_css_selector('#password')
		pass_elem.send_keys(self.password)
		pass_elem.submit()