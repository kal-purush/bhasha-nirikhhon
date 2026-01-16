import allure, time, os
from selenium.webdriver.common.by import By

from selenium.webdriver.support.wait import WebDriverWait


class Base:

    def __init__(self, driver):
        self.driver = driver

    def get_element(self, loc, timeout=30, poll_frequency=1.0):
        """
        定位单个元素
        :param loc: (By.ID,id属性值) (By.CLASS_NAME,class属性值) (By.XPATH,xpath属性值)
        :param timeout: 搜索元素超时时间
        :param poll_frequency: 元素每次搜索间隔
        :return:元素定位对象
        """
        return WebDriverWait(self.driver, timeout, poll_frequency).until(lambda x: x.find_element(*loc))

    def get_elements(self, loc, timeout=30, poll_frequency=1.0):
        """
        定位一组元素
        :param loc: (By.ID,id属性值) (By.CLASS_NAME,class属性值) (By.XPATH,xpath属性值)
        :param timeout: 搜索元素超时时间
        :param poll_frequency: 元素每次搜索间隔
        :return:元素定位对象列表
        """
        return WebDriverWait(self.driver, timeout, poll_frequency).until(lambda x: x.find_elements(*loc))

    def click_element(self, loc, timeout=30, poll_frequency=1.0):
        """
        点击元素
        :param loc: (By.ID,id属性值) (By.CLASS_NAME,class属性值) (By.XPATH,xpath属性值)
        :param timeout: 搜索元素超时时间
        :param poll_frequency: 元素每次搜索间隔
        :return:
        """
        self.get_element(loc, timeout, poll_frequency).click()

    def send_element(self, loc, text, timeout=30, poll_frequency=1.0):
        """
        输入文本内容
        :param loc: (By.ID,id属性值) (By.CLASS_NAME,class属性值) (By.XPATH,xpath属性值)
        :param text: 输入文本内容
        :param timeout: 搜索元素超时时间
        :param poll_frequency: 元素每次搜索间隔
        :return:
        """
        # 定位元素
        input_text = self.get_element(loc, timeout, poll_frequency)
        # 清空输入框
        input_text.clear()
        # 输入元素
        input_text.send_keys(text)

    def scroll_sreen(self, sc=1):
        """
        滑动页面
        :param sc: 1：向上 2：向下 3：向左 4向右
        :return:
        """
        # 防止页面跳转慢
        time.sleep(2)

        # 获取屏幕分辨率 ('width', 'height')
        screen_size = self.driver.get_window_size()
        # 获取宽
        width = screen_size.get("width")
        # 获取高
        height = screen_size.get("height")
        # 根据宽高判断滑动 80% -> 20%

        if sc == 1:
            self.driver.swipe(width * 0.5, height * 0.8, width * 0.5, height * 0.2, 2000)
        if sc == 2:
            self.driver.swipe(width * 0.5, height * 0.2, width * 0.5, height * 0.8, 2000)
        if sc == 3:
            self.driver.swipe(width * 0.8, height * 0.5, width * 0.2, height * 0.5, 2000)
        if sc == 4:
            self.driver.swipe(width * 0.2, height * 0.5, width * 0.8, height * 0.5, 2000)

    def screen_page(self, name="截图"):
        """
        报告添加截图
        :param name: 截图名字
        :return:
        """
        # 定义图片名字
        png_name = "./image" + os.sep + "{}.png".format(int(time.time()))
        # 截图
        self.driver.get_screenshot_as_file(png_name)
        # 二进制打开文件
        with open(png_name, "rb") as f:
            # 使用添加附件 添加到allure报告
            allure.attach("截图名字", f.read(), allure.attach_type.PNG)

        def get_toast(self, toast):
            """
            获取toast消息
            :param self: 拼接xpath的toast内容
            :param toast:
            :return: 返回获取到的toast信息
            """
            toast_xpath = (By.XPATH, "//*[contains(@text,'{}')]".format(toast))
            data = self.get_element(toast_xpath).text
            return data