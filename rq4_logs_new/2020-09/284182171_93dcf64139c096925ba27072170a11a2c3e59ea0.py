#/usr/bin/env phthon
# -*- coding:utf-8 _*_
from selenium_study.selenium_frame_window.base import Base


class TestFile(Base):
    def test_file_upload(self):
        self.driver.get("https://image.baidu.com/")
        self.driver.find_elements_by_xpath("//*[@id='sttb']/img[1]").click
        self.driver.find_elements_by_xpath("//*[@id='stfile']").send_keys()