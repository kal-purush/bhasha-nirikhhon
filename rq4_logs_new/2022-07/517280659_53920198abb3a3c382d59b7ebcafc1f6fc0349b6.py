from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
import time
from StockValues import StockValue

options = FirefoxOptions()
options.add_argument("--headless")

browser = Firefox(options = options)
browser.get("https://www.kauppalehti.fi/porssi/porssikurssit/osake/SPINN")
time.sleep(1)

listItems = browser.find_elements(By.CLASS_NAME, "listItem")

i = 1
for item in listItems:
    print("Item count: ", i)
    if i == 2:
        values2 = item.text.split()
        print("i=2 values: ", values2)
        print("foo_2")
    if i == 3:
        values3 = item.text.split()
        print("i=3 values: ", values3 )
        print("foo_3")
        break;
    i = i+1
stockValues = StockValue("SPINN", values2[1],values2[2],values2[4], values2[5],values3[1], values3[3], values3[5], values3[7])

print("Buy value...")
print(stockValues.getBuyValue())
print("Sell value...")
print(stockValues.getSellValue())


# close web browser
browser.close()