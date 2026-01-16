from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep

# get the path to the ChromeDriver executable
driver_path = ChromeDriverManager().install()

# create a new Chrome browser instance
service = Service(driver_path)
driver = webdriver.Chrome(service=service)
driver.maximize_window()

# open the url
driver.get('https://www.target.com/')


# Click search btn
driver.find_element(By.XPATH, "//span[@class='styles__LinkText-sc-1e1g60c-3 dZfgoT h-margin-r-x3']").click()
sleep(2)
driver.find_element(By.XPATH, "//span[@class='styles__ListItemText-sc-diphzn-1 jaMNVl']").click()
sleep(1)
driver.find_element(By.XPATH, "//input[@autocomplete='username webauthn']")

driver.find_element(By.XPATH, "//div[@class='styles__InputWrapper-sc-17c8y80-0 bUAMkG styles__AuthInput-sc-q9vn5-0 eeoYPi']")

driver.find_element(By.ID, 'username').send_keys('dyak.ilya@gmail.com')
#Need to find the  Sign into your Target account
#and login #