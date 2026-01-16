from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from conftest import *
from data import *
from time import sleep 

def test_data_types_form(chrome_browser):
    chrome_browser.get(url_1)
    chrome_browser.find_element(By.NAME, "first-name").send_keys(first_name)
    chrome_browser.find_element(By.NAME, "last-name").send_keys(last_name)
    chrome_browser.find_element(By.NAME, "address").send_keys(address)
    chrome_browser.find_element(By.NAME, "e-mail").send_keys(email)
    chrome_browser.find_element(By.NAME, "phone").send_keys(phone)
    chrome_browser.find_element(By.NAME, "zip-code").send_keys(zip_code)
    chrome_browser.find_element(By.NAME, "city").send_keys(city)
    chrome_browser.find_element(By.NAME, "country").send_keys(country)
    chrome_browser.find_element(By.NAME, "job-position").send_keys(job_position)
    chrome_browser.find_element(By.NAME, "company").send_keys(company)

    WebDriverWait(chrome_browser, 40, 0.1).until(EC.element_to_be_clickable((By.TAG_NAME, "button"))).click()
    sleep(2)

    assert "danger" in chrome_browser.find_element(By.ID, "zip-code").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "first-name").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "last-name").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "address").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "e-mail").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "phone").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "city").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "country").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "job-position").get_attribute("class")
    assert "success" in chrome_browser.find_element(By.ID, "company").get_attribute("class")
