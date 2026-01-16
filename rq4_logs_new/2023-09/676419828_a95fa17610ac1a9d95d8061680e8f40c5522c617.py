import json
import os
from configuration import BROWSER_LOGS_PATH

def test_logging_browser_chrome(browser):
    browser.execute_script("console.warn('Here is the WARNING message!')")
    browser.execute_script("console.error('Here is the ERROR message!')")
    browser.execute_script("console.log('Here is the LOG message!')")
    browser.execute_script("console.info('Here is the INFO message!')")

    network_log_file = os.path.join(BROWSER_LOGS_PATH, "network.json")
    browser_log_file = os.path.join(BROWSER_LOGS_PATH, "console.json")
    driver_log_file = os.path.join(BROWSER_LOGS_PATH, "driver.json")

    with open(network_log_file, "w") as f:
        logs = browser.get_log("performance")
        data = []
        for entry in logs:
            log = json.loads(entry["message"])["message"]
            if (
                    "Network.response" in log["method"]
                    or "Network.request" in log["method"]
                    or "Network.webSocket" in log["method"]
            ):
                data.append(log)

        f.write(json.dumps(data, indent=4, ensure_ascii=False))

    with open(browser_log_file, "w") as f:
        f.write(json.dumps(browser.get_log("browser"), indent=4, ensure_ascii=False))

    with open(driver_log_file, "w") as f:
        f.write(json.dumps(browser.get_log("driver"), indent=4, ensure_ascii=False))