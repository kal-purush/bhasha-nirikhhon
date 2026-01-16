import pytest
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


@pytest.fixture(scope="module")
def driver():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    yield driver
    driver.quit()


def get_parcel_status(driver, tracking_number):
    driver.get("https://tracking.novaposhta.ua/#/uk")
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//*[@id='en']"))
    ).send_keys(tracking_number)

    # Натискаємо кнопку "Знайти" за новим XPath
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//*[@id='np-number-input-desktop-btn-search-en']"))
    ).click()

    # Отримуємо статус посилки
    parcel_status = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'header__status-text')]"))
    )

    return parcel_status.text


def test_parcel_status(driver):
    tracking_number = "20400396721741"
    expected_status_keyword = "Отримана"  # Ключове слово для перевірки статусу

    actual_status = get_parcel_status(driver, tracking_number)

    assert expected_status_keyword in actual_status, f"Expected '{expected_status_keyword}' in actual status, but got '{actual_status}'"