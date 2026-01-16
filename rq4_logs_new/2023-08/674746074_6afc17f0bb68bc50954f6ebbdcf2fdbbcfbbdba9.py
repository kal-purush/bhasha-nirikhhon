from locators.constructor import ConstructorLocators as l


class Constructor:
    def __init__(self, driver):
        self.driver = driver

    def click_sause_section(self):
        self.driver.find_element(*l.sause_section).click()

    def get_attribute_sause_section(self):
        return self.driver.find_element(*l.sause_section).get_attribute('class')

    def click_filling_section(self):
        self.driver.find_element(*l.filling_section).click()

    def get_attribute_filling_section(self):
        return self.driver.find_element(*l.filling_section).get_attribute('class')

    def click_bun_section(self):
        self.driver.find_element(*l.bun_section).click()

    def get_attribute_bun_section(self):
        return self.driver.find_element(*l.bun_section).get_attribute('class')

