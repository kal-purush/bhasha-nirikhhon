from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from time import sleep

options = Options()
options.add_argument("--disable-extensions")
options.add_argument("--start-maximized")
options.add_argument("user-data-dir=/home/seluser/.config/google-chrome")

browser = webdriver.Remote(
    command_executor="http://selenium-hub:4444/wd/hub",
    desired_capabilities=DesiredCapabilities.CHROME,
    options=options,
)
browser.implicitly_wait(10)

browser.get("https://www.oddspark.com/keiba/")
browser.find_element(By.CSS_SELECTOR, "body")

print(browser.title)
browser.save_screenshot("health.png")

browser.close()
browser.quit()