from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from time import sleep
from webdriver_manager.chrome import ChromeDriverManager

# driver = webdriver.Chrome()
driver = webdriver.Chrome(ChromeDriverManager().install())


# Buka halaman web
driver.get("https://demoqa.com/automation-practice-form")
driver.maximize_window()
sleep(10)

# Mengisi nama depan dan nama belakang
driver.find_element_by_id("firstName").send_keys("Hana")
sleep(2)
driver.find_element_by_id("lastName").send_keys("Maria")
sleep(4)
driver.find_element_by_xpath('//*[@id="userEmail"]').send_keys("hanamaria2307@gmail.com")
sleep(2)

# Memilih jenis kelamin
driver.find_element_by_xpath('//*[@id="genterWrapper"]/div[2]/div[2]').click()
sleep(2)

# Memasukkan nomor ponsel
driver.find_element_by_id("userNumber").send_keys("1234567890")
sleep(2)

# Memilih tanggal lahir
driver.find_element_by_xpath('//*[@id="dateOfBirth"]')
driver.find_element_by_xpath('//*[@id="dateOfBirthInput"]').click()
sleep(2)
driver.find_element_by_xpath('//*[@id="dateOfBirth"]/div[2]/div[2]/div/div/div[2]/div[2]/div[2]/div[4]').click()

sleep(5)
# Memilih subjek
driver.find_element_by_id("subjectsInput").send_keys("Computer Science")
driver.find_element_by_id("subjectsInput").send_keys(Keys.RETURN)
sleep(2)

# Memilih hobi
driver.find_element_by_xpath('//*[@id="hobbiesWrapper"]/div[2]/div[2]').click()
sleep(3)
driver.find_element_by_id("uploadPicture").send_keys("C:/Users/USER/Downloads/tes.jpeg")
sleep(2)
driver.find_element_by_xpath('//*[@id="currentAddress"]').send_keys("test address")
sleep(2)

# Memilih negara dan negara bagian
driver.find_element_by_id("react-select-3-input").send_keys("NCR")
driver.find_element_by_id("react-select-3-input").send_keys(Keys.RETURN)
sleep(2)

driver.find_element_by_id("react-select-4-input").send_keys("Delhi")
driver.find_element_by_id("react-select-4-input").send_keys(Keys.RETURN)
sleep(2)

# # Klik tombol submit
# submit_button.click()
driver.implicitly_wait(20)
driver.find_element_by_id("submit").click()
# driver.find_element_by_xpath('//*[@id="userForm"]/div[11]').click()
# driver.find_element_by_css_selector("btn btn-primary").click()
sleep(5)
