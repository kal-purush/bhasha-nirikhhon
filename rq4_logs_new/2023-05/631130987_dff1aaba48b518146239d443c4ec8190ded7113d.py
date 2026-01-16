import time

# from selenium import webdriver
from selenium.webdriver.chrome import service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.opera.options import Options
from seleniumwire import webdriver
import webdriver as myWebdriver
import json
import pprint
from pyvirtualdisplay import Display

# webdriver_service = service.Service('./operadriver.exe')
# webdriver_service.start()


chrome_options = Options()
chrome_options.add_extension('./TranNamPhong234.crx')
chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
capabilities = webdriver.DesiredCapabilities.CHROME
# capabilities["loggingPrefs"] = {"performance": "ALL"}  # chromedriver < ~75
capabilities["goog:loggingPrefs"] = {"performance": "ALL"}  # chromedriver 75+
# seleniumwire_options = {"addr": "127.0.0.1", "port": 9867}
# driver = webdriver.Remote(webdriver_service.service_url, webdriver.DesiredCapabilities.OPERA, options=chrome_options, seleniumwire_options=seleniumwire_options)
driver = myWebdriver.Opera(executable_path=r'./operadriver.exe', desired_capabilities=capabilities, options=chrome_options)
driver.get('opera://extensions')
time.sleep(5)

driver.get('https://trannamphong.top/Vitaminforsale38b5m9')
# time.sleep(10)


input_password = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type=password]")))

input_password.send_keys('truong193')
btn_login = driver.find_element_by_css_selector('button.passster-submit')
btn_login.click()

tool = WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(text(), 'Tool tìm quảng cáo Facebook/Google/TikTok')]")))
tool.click()

btn_tool_tiktok = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//button[contains(text(), 'Click để sử dụng Tool Spy Tiktok Ads')]")))
btn_tool_tiktok.click()

time.sleep(5)
driver.switch_to.window(driver.window_handles[2])
driver.get('https://www.shoplus.net/discovery/ads')
# driver.get('https://www.shoplus.net/api/v1/tikmeta/portal/ads/search?cursor=0&region=VN&keyword_type=ALL&sort=1&play_count_start=100000')
time.sleep(5)

def process_browser_logs_for_network_events(logs):
    """
    Return only logs which have a method that start with "Network.response", "Network.request", or "Network.webSocket"
    since we're interested in the network events specifically.
    """
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if (
                "Network.response" in log["method"]
                or "Network.request" in log["method"]
                or "Network.webSocket" in log["method"]
        ):
            yield log
logs = driver.get_log("performance")

events = process_browser_logs_for_network_events(logs)
with open("log_entries.txt", "wt") as out:
    for event in events:
        pprint.pprint(event, stream=out)
# header = driver.execute_script("return JSON.stringify(performance.getEntries()[0].request.headers);")
# https://gist.github.com/rengler33/f8b9d3f26a518c08a414f6f86109863c
# print(driver.current_url)
# print(driver.get_cookies())
# print()
# print(driver.requests)
# for request in driver.requests:
# #   print(request)
#   print("request.url\n", request.url) # <--------------- Request url
#   print("request.headers\n", request.headers) # <----------- Request headers
#   print("request.headers.headers\n", request.response.headers) # <-- Response headers
#   print()




# headers = driver.execute_script("var req = new XMLHttpRequest();req.open('GET', document.location, false);req.send(null);return req")

# print(headers)

# headers = headers.splitlines()
# print(headers)
# entries = driver.execute_script("""
#     var performance = window.performance || window.webkitPerformance || window.msPerformance || window.mozPerformance;
#     var network = performance.getEntries() || [];

#     return network;
# """)

# print(entries)
# print("------------------")
# # In ra danh sách các yêu cầu hiện tại
# for entry in entries:
#     # print(entry)
#     if "product_rising_rank" in entry["name"]:
#         print(entry)