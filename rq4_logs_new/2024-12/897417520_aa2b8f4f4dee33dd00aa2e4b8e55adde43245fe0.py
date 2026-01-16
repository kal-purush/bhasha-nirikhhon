from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class ProfilePage:
    def __init__(self, driver):
        self._driver = driver

    def get(self):
        self._driver.get("https://dev.ludio.gg/profile")

    def check_user_email(self):
        try:
            email_element = WebDriverWait(self._driver, 10).until(
            EC.presence_of_element_located((
                By.XPATH,
                '//div[@class="UsernameEmailCustom_u14t02mx"]/p'
            ))
        )
            email_text = email_element.text
            return email_text
        except TimeoutException:
            return None