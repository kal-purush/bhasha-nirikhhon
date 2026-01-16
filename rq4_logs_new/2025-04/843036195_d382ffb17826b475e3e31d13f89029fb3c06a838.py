from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Chrome()

# Abrir Gmail
driver.get("https://mail.google.com/")

# Esperar que cargue el campo de email
wait = WebDriverWait(driver, 10)
email_input = wait.until(EC.presence_of_element_located((By.ID, "identifierId")))
email_input.send_keys("matricula@uaz.edu.mx")  # Reemplázalo con tu correo
email_input.send_keys(Keys.RETURN)

# Esperar que aparezca el campo de contraseña
password_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@aria-label='Ingresa tu contraseña']")))
password_input.send_keys("contraseña")  # Reemplázalo con tu contraseña
password_input.send_keys(Keys.RETURN)

# Esperar a que cargue la bandeja de entrada
wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nH")))  # Gmail carga elementos con la clase "nH"

print("Inicio de sesión exitoso")

# Esperar unos segundos y cerrar
time.sleep(5)
driver.quit()
