from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# WebDriver settings (replace with the path to your driver)
driver = webdriver.Chrome()

# Opening HTML page through local server
driver.get('http://localhost:8000/dz.html')


# Function to work with frames
def handle_frame(frame_id, input_id, secret_text):
    time.sleep(3)  # Adding delay
    driver.switch_to.frame(driver.find_element(By.ID, frame_id))

    time.sleep(3)  # Adding delay
    input_element = driver.find_element(By.ID, input_id)
    input_element.send_keys(secret_text)

    time.sleep(3)  # Adding delay
    check_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Перевірити')]")
    check_button.click()

    time.sleep(3)  # Adding delay
    alert = driver.switch_to.alert
    alert_text = alert.text
    print(f'Alert text for {frame_id}: {alert_text}')
    alert.accept()

    driver.switch_to.default_content()


# Working with the first frame
handle_frame('frame1', 'input1', 'Frame1_Secret')

# Working with the second frame
handle_frame('frame2', 'input2', 'Frame2_Secret')

# Adding a success message
print("Test passed successfully!")

# Closing the browser
driver.quit()