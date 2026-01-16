import time

from base.base_test import BaseTest

class TestChangeProfile(BaseTest):

    def test_change_profile(self):
        self.login_page.open()
        self.login_page.enter_login(self.data.LOGIN)
        self.login_page.enter_password(self.data.PASSWORD)
        self.login_page.click_submit_button()
        time.sleep(3)

