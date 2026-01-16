from selenium import webdriver
from time import sleep


def _test_some(browser):
    browser.get('https://kirov.vseinstrumenti.ru/instrument/pily/tsirkulyarnye_diskovye/elitech/diskovaya_pila_elitech_pd_2000s/')
    lovi_moment_btn = browser.find_element_by_css_selector('div.price-falldown-sale-block__header')
    print(lovi_moment_btn.text)
    if lovi_moment_btn:
        item_price = browser.find_element_by_css_selector('#card-month-sale-block > div.month-sale-block__body.ns > span.price-value')
        print(item_price.text)
    no_item_btn = browser.find_element_by_css_selector('#b-product-info > div.right-block.card-right-aside > div.card-basket-block-new > div:nth-child(6) > span')
    print(no_item_btn.text)
    item_price = browser.find_element_by_css_selector('#b-product-info > div.right-block.card-right-aside > div.card-basket-block-new > div.ns.price-wrapper > div > span.price-value')
    print(item_price)



if __name__ == '__main__':
    browser = webdriver.Chrome()
    _test_some(browser)