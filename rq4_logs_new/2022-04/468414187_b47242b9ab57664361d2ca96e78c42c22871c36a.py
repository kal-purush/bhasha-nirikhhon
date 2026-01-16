#cambiar lo que pueda xpath por name
from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.firefox.options import Options
import time
class MySeleniumTests(StaticLiveServerTestCase):
    fixtures = ['testdb.json',]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # així és com ho diu a la doc de Django
        # però necessitem una altra configuració pel mode headless
        #cls.selenium = WebDriver()
        #cls.selenium.implicitly_wait(5)
        opts = Options()
        opts.headless = False
        cls.selenium = WebDriver(options=opts)
        cls.selenium.implicitly_wait(5)

    @classmethod
    def tearDownClass(cls):
        cls.selenium.quit()
        super().tearDownClass()

    def test_login(self):
        self.selenium.get('%s%s' % (self.live_server_url, '/admin/login/'))
        username_input = self.selenium.find_element_by_name("username")
        username_input.send_keys('cf19lluis.meca@iesjoandaustria.org')
        password_input = self.selenium.find_element_by_name("password")
        password_input.send_keys('lluismeca')
        time.sleep(2)
        self.selenium.find_element_by_xpath('//input[@value="Log in"]').click()