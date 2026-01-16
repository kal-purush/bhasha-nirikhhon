from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

class Ticket():
    """
    抢票
    """
    def __init__(self):
        self.path = 'E:\leidownload\chromedriver_win32\chromedriver.exe'
        self.driver=webdriver.Chrome(executable_path=self.path)
        self.longin_url='https://kyfw.12306.cn/otn/resources/login.html'
        self.index_url='https://kyfw.12306.cn/otn/view/index.html'
        self.ticket='https://kyfw.12306.cn/otn/leftTicket/init?linktypeid=dc'
        self.order_html='https://kyfw.12306.cn/otn/confirmPassenger/initDc'
        self.wait_time=1

    def _info(self):
        self.start_station=input('请输入起点：')
        self.final_station=input('请输入终点：')
        self.datatime=input('乘车日期（格式必须为Y-M-D）：')
        self.train_number=input('车次(如果有多个车次请用英文的逗号进行分割)：').split(',')
        self.chair=input('选择类型(如果有选择多个作为请用英文逗号进行分割)：').split(',')
        self.passager=input('乘客（如果有多名乘客请用英文的逗号进行分割）：').split(',')

    def _login(self):
        self.driver.get(self.longin_url)
        #显示等待 等待用户登陆后的页面
        #隐式等待
        WebDriverWait(self.driver,1000).until(EC.url_to_be(self.index_url))
        print('登陆成功')


    def _tiket_check(self):
        #登陆成功后跳转到查询余票的页面
        self.driver.get(self.ticket)

        try:
            #等待出发地是否正确
            WebDriverWait(self.driver,1000).until(
                EC.text_to_be_present_in_element_value((By.ID,'fromStationText'),self.start_station)
            )

            #等待目的地是否输入正确
            WebDriverWait(self.driver,1000).until(
                EC.text_to_be_present_in_element_value((By.ID,'toStationText'),self.final_station))

            #等待时间是否输入正确
            WebDriverWait(self.driver,1000).until(
                EC.text_to_be_present_in_element_value((By.ID,'train_date'),self.datatime)
            )

            #判断查询按钮是否可以被点击
            WebDriverWait(self.driver,1000).until(
                EC.element_to_be_clickable((By.ID,'query_ticket'))
            )

        except Exception as e:
            print('输入的信息有误，请重新输入')

        #如果都正确点击查询按钮
        lookfor_btn=self.driver.find_element_by_id('query_ticket')
        # lookfor_btn.click()
        #等待点击查询按钮后 等待车次信息是否显示出来了
        while lookfor_btn:
            lookfor_btn.click()
            lookfor_btn=self.driver.find_element_by_id('query_ticket')
            WebDriverWait(self.driver,1000).until(EC.presence_of_element_located(
                (By.XPATH,'//tbody[@id="queryLeftTable"]/tr')))
            tr_list=self.driver.find_elements_by_xpath('//tbody[@id="queryLeftTable"]/tr[not(@datatran)]')

            for tr in tr_list:
                train_num=tr.find_element_by_class_name('number').text
                if train_num in self.train_number:
                    is_seat=tr.find_element_by_xpath('.//td[4]').text
                    if is_seat != '无' and is_seat != '--':
                        print('%s躺列车是有座的'%train_num)
                        reserve_btn=tr.find_element_by_xpath('.//a[@class="btn72"]')
                        reserve_btn.click()
                        #等待订票页面加载完成 选择乘车人
                        WebDriverWait(self.driver,1000).until(
                            EC.url_to_be(self.order_html))
                        #加载完成后选择乘车人 进行点击

                        #todo:等待所有乘客加载完成后找到乘客的姓名  这里一定得等到页面信息加载完成后才能进行循环遍历
                        WebDriverWait(self.driver,1000).until(
                            EC.presence_of_element_located((By.XPATH,'.//ul[@id="normal_passenger_id"]/li/label'))
                        )

                        passengers_list=self.driver.find_elements_by_xpath('.//ul[@id="normal_passenger_id"]/li/label')
                        for label in passengers_list:
                            if label.text in self.passager:
                                label.click()
                        #点击确认按钮
                        self.driver.find_element_by_id('submitOrder_id').click()

                        WebDriverWait(self.driver, 1000).until(
                            EC.presence_of_element_located((By.ID, 'content_checkticketinfo_id'))
                        )
                        #等待模态对话框加载出来以后点击确认按钮
                        WebDriverWait(self.driver,1000).until(
                                EC.presence_of_element_located((By.ID,'qr_submit_id'))
                            )

                        confirm_btn=self.driver.find_element_by_id('qr_submit_id')
                        print(confirm_btn)
                        time.sleep(0.5)
                        confirm_btn.click()

                        while confirm_btn:
                            print('一直在点击')
                            confirm_btn.click()
                            confirm_btn = self.driver.find_element_by_id('qr_submit_id')
                        print('抢票成功，庆祝！！！！！！！')
                        break
            print('重复点击了')
            time.sleep(self.wait_time)

    def run(self):
        self._info()
        self._login()
        self._tiket_check()

if __name__ == '__main__':
    qiang=Ticket()
    qiang.run()