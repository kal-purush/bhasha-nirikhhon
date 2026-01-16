from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@task
def selenium_task():
    options = Options()
    options.add_argument("--headless")  # Run Chrome in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Create an instance of the ChromeDriver with the configured options
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    # Navigate to a web page
    driver.get("https://www.selenium.dev/selenium/web/web-form.html");

    # Perform actions in the browser
    title = driver.title

    # Capture a screenshot of the page
    driver.save_screenshot("screenshot.png")

    # Close the browser
    driver.quit()

@dag(default_args={"start_date": days_ago(1)})
def selenium_test_dag():
    selenium_task()

dag = selenium_test_dag()

#test2