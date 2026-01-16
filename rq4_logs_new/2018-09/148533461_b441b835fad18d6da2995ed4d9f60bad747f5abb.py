import unittest
from os import path, getcwd
from driver_picker import DriverPicker
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class PrivacyButtonTest(unittest.TestCase):
    def setUp(self):
        self.GoogleDriver = webdriver.Chrome(DriverPicker().driver_version)
        self.GoogleDriver.get("file://" + path.join(getcwd(), 'public', 'index.html'))

    def test_privacy_policy_page_source(self):
        driver = self.GoogleDriver
        assert "Privacy Policy" in driver.page_source

    def test_privacy_button_display(self):
        driver = self.GoogleDriver
        assert driver.find_element_by_id("privacy-button").is_displayed()

    def test_privacy_button_enabled(self):
        driver = self.GoogleDriver
        assert driver.find_element_by_id("privacy-button").is_enabled()


    def test_privacy_button_link(self):
        driver = self.GoogleDriver
        element = driver.find_element_by_id("privacy-button").click()
        driver.implicitly_wait(1)
        assert "#privacy" in driver.current_url

    def test_privacy_policy_display(self):
        driver = self.GoogleDriver
        assert driver.find_element_by_id("privacy").is_displayed()

    def test_privacy_policy_enabled(self):
        driver = self.GoogleDriver
        assert driver.find_element_by_id("privacy").is_enabled()

    def tearDown(self):
        self.GoogleDriver.close()

