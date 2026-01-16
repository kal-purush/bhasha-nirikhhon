from selenium import webdriver
from selenium.webdriver.common.by import By
import os

chrome_webdriver = os.environ["CHROME_DRIVER"]
URL = "chrome://dino/"


class DinosaurGameBot:
    def __init__(self):
        self.driver = webdriver.Chrome(executable_path=chrome_webdriver)
        self.driver.get(URL)