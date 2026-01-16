'''
Created on Fri Jun 24 2016 15:57:15 GMT-0700 (PDT)

@author:Judy Yang
'''
from wtframework.wtf.web.page import PageObject, InvalidPageError
from wtframework.wtf.web.webdriver import WTF_CONFIG_READER
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from wtframework.wtf.config import WTF_TIMEOUT_MANAGER
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import random


class Fashion(PageObject):
    '''
    Fashion
    WTFramework PageObject representing a page like:
    https://qa.emfitting.com/Fashion%20News.php
    '''


    ### Page Elements Section ###
    ### End Page Elements Section ###

    base_url = WTF_CONFIG_READER.get("baseurl")
    fashion_url = base_url+'Fashion%20News.php'

    def _validate_page(self, webdriver):
        '''
        Validates we are on the correct page.
        '''

        if not self.base_url+'Fashion%20News.php' in webdriver.current_url:
            raise InvalidPageError("This page did not pass Fashion page validation.")

    def goto_fashion_news(self):
        self.webdriver.find_element_by_link_text("Fashion News").click()
        WTF_TIMEOUT_MANAGER.brief_pause()   
            
        return self.webdriver.current_url == self.fashion_url+'?classid=1'

    def goto_article_in_fashion_news(self):
        self.webdriver.find_element_by_link_text("The Trouble With Modern-Day Models").click()
        WTF_TIMEOUT_MANAGER.brief_pause()

        return self.webdriver.current_url == self.base_url+'news.php?id=63'

    def goto_dress_code(self):
        self.webdriver.get(self.fashion_url)
        self.webdriver.find_element_by_link_text("Dress Code").click()
        WTF_TIMEOUT_MANAGER.brief_pause()   
            
        return self.webdriver.current_url == self.fashion_url+'?classid=2'

    def goto_article_in_dress_code(self):
        self.webdriver.find_element_by_partial_link_text("Dress Codes & What They Mean").click()
        WTF_TIMEOUT_MANAGER.brief_pause()

        return self.webdriver.current_url == self.base_url+'news.php?id=58'

    def goto_style_creating(self):
        self.webdriver.get(self.fashion_url)
        self.webdriver.find_element_by_link_text("Style Creating").click()
        WTF_TIMEOUT_MANAGER.brief_pause()   
            
        return self.webdriver.current_url == self.fashion_url+'?classid=3'

    def goto_article_in_style_creating(self):
        self.webdriver.find_element_by_link_text("A Very Simple Guide to Tie Knots").click()
        WTF_TIMEOUT_MANAGER.brief_pause()

        return self.webdriver.current_url == self.base_url+'news.php?id=59'

    def goto_social_manners(self):
        self.webdriver.get(self.fashion_url)
        self.webdriver.find_element_by_link_text("Social Manners").click()
        WTF_TIMEOUT_MANAGER.brief_pause()   
            
        return self.webdriver.current_url == self.fashion_url+'?classid=4'

    def goto_article_in_social_manners(self):
        self.webdriver.find_element_by_link_text("Small Business Manners & Etiquette Rules To Never Forget").click()
        WTF_TIMEOUT_MANAGER.brief_pause()

        return self.webdriver.current_url == self.base_url+'news.php?id=60'
    
    def fashion_test(self):
        f = open("debug_fashion.txt", "w")
        test1 = self.goto_fashion_news()
        f.write("test1: "+str(test1))
        test2 = self.goto_article_in_fashion_news()
        f.write("\ntest2: "+str(test2))
        test3 = self.goto_dress_code()
        f.write("\ntest3: "+str(test3))
        test4 = self.goto_article_in_dress_code()
        f.write("\ntest4: "+str(test4))
        test5 = self.goto_style_creating()
        f.write("\ntest5: "+str(test5))
        test6 = self.goto_article_in_style_creating()
        f.write("\ntest6: "+str(test6))
        test7 = self.goto_social_manners()
        f.write("\ntest7: "+str(test7))
        test8 = self.goto_article_in_social_manners()
        f.write("\ntest8: "+str(test8))
        return test1 and test2 and test3 and test4 and test5 and test6 and test7 and test8