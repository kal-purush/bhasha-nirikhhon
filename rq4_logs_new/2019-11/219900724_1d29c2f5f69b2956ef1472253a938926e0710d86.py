#coding=utf-8
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver



#类的使用，class
class seleniumDriver:

    def __init__(self,browser):
        self.driver = self.open_browser(browser)

    def element_isdisplay(self,element):
        flag = element.is_displayed()
        if flag == True:
            return element
        else:
            return False

    def open_browser(self,browser):
        #选择哪个浏览器进行调用
        try:
            if browser == 'chrome':
                driver = webdriver.Chrome()
            elif browser == 'firefox':
                driver = webdriver.Firefox()
            elif browser == 'ie':
                driver = webdriver.Ie()
            else:
                driver = webdriver.Edge()
            time.sleep(5)
            return driver
        except:
            print('打开浏览器失败')
            return None

    def get_url(self,url):
        '''
        判断URL是否有问题
        '''
        if self.driver != None:
            if 'http://'in url:
                self.driver.get(url)
            else:
                print("你的URL有问题")
        else:
            print('case失败')


    def handle_windows(self,*args):
        '''
        调整窗口大小
        '''
        value = len(args)
        if value == 1:
            if args[0] == 'max':
                self.driver.maximize_window()
                print('x')
            elif args[0] == 'back':
                self.driver.back()
            elif args[0] == 'forward':
                self.driver.forward()
            elif args[0] == 'refresh':
                self.driver.refresh()
            else:
                self.driver.minimize_window()
                print('y')
        elif value ==2:
            self.driver.set_window_size(args[0],args[1])
            print('Z')
        else:
            print('传参有误')
        time.sleep(5) 
        self.driver.quit()

    def assert_title(self,title_name = None):
        
        #判断title是否正确
        if title_name !=None:  
            get_title = EC.title_contains(title_name)
            return get_title(self.driver)

    def open_url_is_true(self,url,title_name=None):#可能没有参数的放在后面
        #先判断URL的正确，然后再判断title是否正确
        self.get_url(url)
        return self.assert_title(title_name)
    
    def close_driver(self):
        self.driver.quit()
    
    def switch_windows(self,title_name=None):
       #切换窗口
        handl_list = self.driver.window_handles
        current_handle = self.driver.current_window_handle
        for i in handl_list:
            if i != current_handle:
                time.sleep(1)
                self.driver.switch_to.window(i)
                if self.assert_title(title_name):
                    break

    def get_element(self,by,value):

        # 获取元素element
        # @parame by 定位方式
        # @parame value 定位位置
        # @return element 返回一个元素

        element = None
        try:
            if by == 'id':
                element = self.driver.find_element_by_id(value)
            elif by == 'name':
                element = self.driver.find_element_by_name(value)
            elif by == 'css':
                element = self.driver.find_element_by_css_selector(value)
            elif by == 'class':
                element = self.driver.find_element_by_class_name(value)
            else:
                element = self.driver.find_element_by_xpath(value)
        except:
            print("定位方式：",by,"定位值：",value,"定位出现错误没有成功")
        return self.element_isdisplay(element)

    def get_elements(self,by,value):

        # 获取元素elements
        # @parame by 定位方式
        # @parame value 定位位置
        # @return elements 返回一个元素

        elements = None
        element_list = []
        if by == 'id':
            elements = self.driver.find_elements_by_id(value)
        elif by == 'name':
            elements = self.driver.find_elements_by_name(value)
        elif by == 'css':
            elements = self.driver.find_elements_by_css_selector(value)
        elif by == 'class':
            elements = self.driver.find_elements_by_class_name(value)
        else:
            elements = self.driver.find_elements_by_xpath(value)
        for element in elements:
            if self.element_isdisplay(element) == False:
                continue
            else:
                element_list.append(element_list)
        return element_list

    def get_level_element(self,by,value,node_by,node_value):

        #层级定位，通过父节点找子节点

        element = self.get_element(by,value)
        if node_by == 'id':
            element = element.find_element_by_id(node_value)
        elif by == 'name':
            element = element.find_element_by_name(node_value)
        elif by == 'css':
            element = element.find_element_by_css_selector(node_value)
        elif by == 'class':
            element = element.find_element_by_class_name(node_value)
        else:
            element = element.find_element_by_xpath(node_value)
        return self.element_isdisplay(element)

    def get_list_element(self,by,value,index):

        #通过list定位我们的元素

        elements = self.get_elements(by,value)
        if index > len(elements):
            return None
        return elements[index]
    
    def send_value(self,by,value,key):

        #输入值

        element = self.get_element(by,value)

        if element != None:
            element.send_keys(key)
        else:
            print("输入失败，定位元素没有找到")
        
    def click_element(self,by,value):

        #点击元素

        element = self.get_element(by,value)
        if element !=None:
            element.click()
        else:
            print("点击失败，定位元素没有找到")
    
    def check_box_isselected(self,by,value,check=None ):

        #判断是否选中

        element = self.get_element(by,value)
        flag = element.is_selected()
        if flag == True:
            if check != 'check':
                self.click_element(by,value)
        else:
            if check == 'check':
                self.click_element(by,value)

            
                
            


selfnium_driver = seleniumDriver('chrome')
#selfnium_driver.handle_windows('max')
#print(selfnium_driver.open_url_is_true('http://www.imooc.com','程序员'))

selfnium_driver.get_url('http://www.imooc.com')
selfnium_driver.get_element('id','username')
selfnium_driver.send_value('id','name','test')
time.sleep()
selfnium_driver.close_driver()