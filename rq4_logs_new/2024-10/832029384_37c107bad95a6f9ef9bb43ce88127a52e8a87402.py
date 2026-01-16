from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class LoginPage:
    URL = "https://guest:welcome2qauto@qauto2.forstudy.space"
    SIGNIN_BUTTON = (By.XPATH, "//button[contains(@class, 'header_signin')]")
    REGISTER_LINK = (By.XPATH, "//button[text()='Registration']")

    def __init__(self, driver):
        self.driver = driver

    def load(self):
        self.driver.get(self.URL)
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.SIGNIN_BUTTON)
        )

    def click_sign_in(self):
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable(self.SIGNIN_BUTTON)
        ).click()

    def click_register(self):
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable(self.REGISTER_LINK)
        ).click()


class RegistrationPage:
    FIRST_NAME_INPUT = (By.ID, 'signupName')
    LAST_NAME_INPUT = (By.ID, 'signupLastName')
    EMAIL_INPUT = (By.ID, 'signupEmail')
    PASSWORD_INPUT = (By.ID, 'signupPassword')
    CONFIRM_PASSWORD_INPUT = (By.ID, 'signupRepeatPassword')
    REGISTER_BUTTON = (By.XPATH, "//button[@class='btn btn-primary']")
    SUCCESS_MESSAGE = (By.XPATH, "//p[text()='Registration complete']")
    ERROR_MESSAGE = (By.XPATH, "//*[@class='alert alert-danger']")

    def __init__(self, driver):
        self.driver = driver

    def register(self, first_name, last_name, email, password):
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.FIRST_NAME_INPUT)
        ).send_keys(first_name)
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.LAST_NAME_INPUT)
        ).send_keys(last_name)
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.EMAIL_INPUT)
        ).send_keys(email)
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.PASSWORD_INPUT)
        ).send_keys(password)
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.CONFIRM_PASSWORD_INPUT)
        ).send_keys(password)
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable(self.REGISTER_BUTTON)
        ).click()

    def get_success_message(self):
        return WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.SUCCESS_MESSAGE)
        ).text

    def get_error_message(self):
        return WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.ERROR_MESSAGE)
        ).text

    def register_and_get_message(self, first_name, last_name, email, password):
        self.register(first_name, last_name, email, password)
        try:
            return self.get_success_message()
        except:
            return self.get_error_message()