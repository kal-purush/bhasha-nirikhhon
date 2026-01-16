import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import pytest


@pytest.fixture(scope="module")
def driver():
    driver = webdriver.Chrome()
    yield driver
    driver.quit()


def test_flight_status(driver):
    driver.get("https://www.flightradar24.com/data/airports/pnq")
    WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//a[contains(text(),'Show all arrivals')]")))
    cities = ["Bangalore", "Delhi", "Goa", "Chandigarh", "Hyderabad", "Nagpur", "Dubai"]
    print("code check")

    for city in cities:
        try:
            flight_status = driver.find_element(By.XPATH,f"//div//label[text()='Departures ']//preceding::span[contains(text(),'{city}')]/ancestor::td/following-sibling::td[4]")
            print(f"{city}:{flight_status.text}")
        except:
            print(f"{city}: data not available")
