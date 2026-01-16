from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Coin:

    def __init__(self, url):
        self.driver = webdriver.Chrome()
        self.url = url

    def main(self):
        self.driver.get(self.url)
        one_hour = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, '//div[@id="1h"]')
            ))
        self.driver.execute_script("arguments[0].click();", one_hour)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, '//span[@key="change"]')
            ))
        while True:
            html = self.driver.page_source
            soup = BeautifulSoup(html, "lxml")
            change = soup.find('span', class_="default-label-box title_change_value")
            change = float(change.text[:-1])
            if change <= -1:
                print(change, datetime.now())


if __name__ == '__main__':
    a = Coin('https://www.binance.com/ru/futures/XRPUSDT?_from=markets')
    a.main()