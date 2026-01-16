from selenium.webdriver.common.by import By


class LoginpageLocators:
    """A class for main page locators.All main page locators should come here"""
    INPUT_USERNAME = (By.ID, 'username')
    INPUT_PASSWORD = (By.ID, 'password')
    BUTTON_LOGIN = (By.XPATH, "//button[@type='submit']")