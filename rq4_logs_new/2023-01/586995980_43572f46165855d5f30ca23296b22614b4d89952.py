"""Test related to registration form and process"""
import logging
import random
import string
import time

from selenium import webdriver
from selenium.webdriver.common.by import By


class TestStartPage:
    """Stores test for registration form base functionality"""
    log = logging.getLogger("[StartPage]")

    @staticmethod
    def random_num():
        """Generates random number"""
        return str(random.randint(111111, 999999))

    @staticmethod
    def random_str(length=5):
        """Generates random string"""
        return ''.join(random.choice(string.ascii_letters) for _ in range(length))

    def test_register(self):
        """
        - Pre-condition:
            -Open start page
        -Steps:
            -Fill login
            -Fill Password
            -Click on SignIn button
            -Verify error
        """
        # open start page
        driver = webdriver.Chrome(executable_path="C:\chromedriver\chromedriver.exe")
        driver.get("https://qa-complexapp.onrender.com/")
        self.log.info("Start page was opened")
        time.sleep(1)

        # Fill in Login
        login_field = driver.find_element(By.XPATH, ".//input[@placeholder='Username']")
        login_field.clear()
        login_field.send_keys("testuser1234")
        self.log.info("Login was filled")
        time.sleep(1)

        # Fill in password
        password_field = driver.find_element(By.XPATH,
                                             ".//input[@name='password'][@class='form-control form-control-sm input-dark']")
        password_field.clear()
        password_field.send_keys("testuser1234")
        self.log.info("Password was filled")
        time.sleep(1)

        # Click on SignIn button
        sign_in = driver.find_element(By.XPATH, ".//button[text()='Sign In']")
        sign_in.click()
        self.log.info("Sign in was clicked")
        time.sleep(1)

        # Verify error
        error_message = driver.find_element(By.XPATH, ".//div[@class='alert alert-danger text-center']")
        assert error_message.text == 'Error'
        driver.close()

    def test_registration_form(self, execution_number):
        """
        - Pre-condition:
            -Open start page, registration form
        -Steps:
            -Fill username
            -Fill email address
            -Fill Password
            -Click on "Sign Up for OurApp" button
            -Verify transfer to personal page
        """

        # open start page
        driver = webdriver.Chrome(executable_path="C:\chromedriver\chromedriver.exe")
        driver.get("https://qa-complexapp.onrender.com/")
        self.log.info("Start page was opened")
        time.sleep(1)

        # Fill in Login
        username_field = driver.find_element(by=By.XPATH, value=".//input[@id='username-register']")
        username_field.clear()
        username = f"testuser{self.random_num()}"
        username_field.send_keys(username)
        time.sleep(1)
        self.log.info("Username was filled")

        # Fill in email
        email_field = driver.find_element(by=By.XPATH, value=".//input[@id='email-register']")
        email_field.clear()
        email_field.send_keys(f"{username}@user.com")
        self.log.info("Email was filled")
        time.sleep(1)

        # Fill in password
        password_field = driver.find_element(by=By.XPATH, value=".//input[@id='password-register']")
        password_field.clear()
        password_field.send_keys(f"pass{username}")
        self.log.info("Password was filled")
        time.sleep(2)

        # Click on Sign Up button
        sign_up = driver.find_element(by=By.XPATH, value=".//button[text()='Sign up for OurApp']")
        sign_up.click()
        self.log.info("Sign Up was clicked")
        time.sleep(1)

        # Verify transfer to personal page
        page_title = driver.find_element(by=By.XPATH, value=f".//span[text()=' {username}']")
        assert page_title.text == username
        self.log.info("Transfer  to personal page (registration) was verified for - ", username)
        driver.close()