from selenium import webdriver

class DriverUtils:
    @staticmethod
    def initialize_driver():
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        driver.set_window_size(width=1024, height=990)
        return driver