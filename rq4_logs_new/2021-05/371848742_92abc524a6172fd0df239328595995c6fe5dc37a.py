import sys
import unittest
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.common.by import By
import re


class TestRetoFinalCaso(unittest.TestCase):
    def setUp(self) -> None:
        self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(30)
        self.driver.maximize_window()

    def tearDown(self) -> None:
        self.driver.close()

    def test_reto_final_caso_arbitrario1(self):
        driver = self.driver
        driver.get("https://demoqa.com/text-box")
        time.sleep(3)

        email = driver.find_element_by_xpath("//input[@id='userEmail']")
        button = driver.find_element_by_xpath("//button[@id='submit']")

        email.send_keys("luisleef@gmail.com")
        button.click()
        label = driver.find_element_by_xpath("//p[@id='email']")
        time.sleep(3)
        self.assertEqual(label.text, "Email:"+"luisleef@gmail.com")

        # codigo here!


    def test_reto_final_caso_arbitrario2(self):
        driver = self.driver
        driver.get("https://demoqa.com/login")
        button = driver.find_element_by_xpath("//button[@id='login']")
        button.click()
        time.sleep(2)
        username = driver.find_element_by_xpath("//input[@id='userName']")
        print(username.value_of_css_property("border-color"))
        time.sleep(3)
        self.assertEqual(str(username.value_of_css_property("border-color")), "rgb(220, 53, 69)")

    def test_reto_final_caso_arbitrario3(self):
        driver = self.driver
        driver.get("https://demoqa.com/automation-practice-form")
        number = driver.find_element_by_xpath("//input[@id='userNumber']")
        button = driver.find_element_by_xpath("//button[@id='submit']")
        time.sleep(3)
        number.send_keys("8755524578")
        button.click()
        number_after = driver.find_element_by_xpath("//input[@id='userNumber']")
        time.sleep(3)

        self.assertTrue(len(str(number_after.get_attribute("value"))) == 10, str(number_after.get_attribute("value")).isnumeric() )

    def test_reto_final_caso_arbitrario4(self):
        driver = self.driver
        driver.get("https://demoqa.com/links")
        link = driver.find_element_by_xpath("//a[@id='forbidden']")
        link.click()
        link_response = driver.find_element_by_xpath("//p[@id='linkResponse']")
        time.sleep(3)

        self.assertRegex(link_response.text, 'Forbidden' )

    def test_reto_final_caso_arbitrario5(self):
        driver = self.driver
        driver.get("https://demoqa.com/radio-button")

        radio_button = driver.find_element_by_xpath(
            "//label[contains(text(),'Impressive')]"
        )
        radio_button.click()
        show_message = driver.find_element_by_xpath("//span[contains(text(),'Impressive')]")
        time.sleep(3)
        self.assertRegex(show_message.text, "Impressive")


if __name__ == '__main__':
    unittest.main()