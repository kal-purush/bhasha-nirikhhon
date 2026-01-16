''' pip install selenium, translate, bs4, pandas, requests, 
'''
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from translate import Translator
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
import random
import time



url = 'https://www.ikea.com/th/th/cat/double-beds-16284/'

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}
options = webdriver.ChromeOptions()
options.add_argument(
    f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

service = Service(r'E:\OSPanel\domains\selenium\chromedriver\chromedriver.exe')
options.binary_location = r"D:\Program Files\Google\Chrome\Application\chrome.exe"

translator_th = Translator(from_lang="th", to_lang="en")


def get_source_html(url):

    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)
        time.sleep(5)

        while True:
            if driver.find_elements(By.XPATH, '/html/body/main/div/div/div[4]/div[1]/div/div[3]/a'):
                element = driver.find_element(By.XPATH, '/html/body/main/div/div/div[4]/div[1]/div/div[3]/a')
                # спасение от проблем с нажатием
                driver.execute_script("arguments[0].click();", element)
                time.sleep(3)
            else:
                with open('index.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                break

    except Exception as ex:
        print(ex)
    finally:
        driver.close()
        driver.quit()

    return '[INFO] Sourse html collected successfully!'


def get_item_urls():

    with open('index.html', 'r', encoding='utf-8') as f:
        src = f.read()

    soup = bs(src, 'lxml')

    items_cards = soup.find_all('div', class_='plp-fragment-wrapper')

    urls = []
    for item_url in items_cards:
        item_url = item_url.find('a').get('href')
        urls.append(item_url)

    with open('items_urls.txt', 'w', encoding='utf-8') as f:
        for url in urls:
            f.write(f'{url}\n')

    return '[INFO] Urls collected successfully!'


def get_data():
    with open('items_urls.txt', 'r', encoding='utf-8') as f:
        urls_list = [url.strip() for url in f.readlines()]

    result_list = []

    result_list.append(
        {
            'Group No': 'Optional',
            'Category': 'Mandatory',
            'Product Name': 'Mandatory',
            'Product Name in EN': 'Optional',
            'Product Images1': 'Mandatory',
            'Product Images2': 'Optional',
            'Product Images3': 'Optional',
            'Product Images4': 'Optional',
            'Product Images5': 'Optional',
            'Product Images6': 'Optional',
            'Product Images7': 'Optional',
            'Product Images8': 'Optional',
            'Brand': 'Mandatory',
            'Long Description': 'Optional',
            'Dangerous Goods': 'Optional',
            'Variation Name1': 'Optional',
            'Option for Variation1': 'Optional',
            'Image per Variation': 'Optional',
            'Variation Name2': 'Optional',
            'Option for Variation2': 'Optional',
            'Package Weight (kg)': 'Mandatory',
            'Stock': 'Mandatory',
            'Price': 'Mandatory',
            'SellerSKU': 'Optional',
            'Package Length (cm)': 'Mandatory',
            'Package Width (cm)': 'Mandatory',
            'Package Height (cm)': 'Mandatory',
        }
    )
    result_list.append(
        {
            'Group No': 'Group number enables the grouping of multiple variations as one product. Group numbers are processed in neighbouring row sequence. Please be informed that blanks are considered a value for group number.',
            'Category': 'Indicate the appropriate category ID for each product. An accurate category ID would boost search results.',
            'Product Name': 'Product name should include product brand and model. Avoid irrelevant keywords as it may cause the listing to be banned.',
            'Product Name in EN': 'Product name should include product brand and model. Avoid irrelevant keywords as it may cause the listing to be banned.',
            'Product Images1': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images2': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images3': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images4': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images5': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images6': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images7': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Product Images8': 'The URL of your main product image. The image cannot be duplicated with other listings in your shop',
            'Brand': 'The brand of your product',
            'Long Description': 'A good product description enhances the quality of your listing and increases chances of sales.',
            'Dangerous Goods': 'Please fill in accurately. Inaccurate DG may result in additional shipping fee or failed delivery. DG contains battery/magnet/liquid/flammable materials. If not fill, default value is None DG.',
            'Variation Name1': 'Add variants to a product that has more than one option. You can select from the provided variant types or create your own. <a href="https://university.lazada.co.th/course/learn?spm=a1zawg.20777315.0.0.37894edfHY3Wmc&id=1000&type=article" target="_blank">Learn More</a>',
            'Option for Variation1': 'Add variants to a product that has more than one option. You can select from the provided variant types or create your own. <a href="https://university.lazada.co.th/course/learn?spm=a1zawg.20777315.0.0.37894edfHY3Wmc&id=1000&type=article" target="_blank">Learn More</a>',
            'Image per Variation': 'Add variants to a product that has more than one option. You can select from the provided variant types or create your own. <a href="https://university.lazada.co.th/course/learn?spm=a1zawg.20777315.0.0.37894edfHY3Wmc&id=1000&type=article" target="_blank">Learn More</a>',
            'Variation Name2': 'Add variants to a product that has more than one option. You can select from the provided variant types or create your own. <a href="https://university.lazada.co.th/course/learn?spm=a1zawg.20777315.0.0.37894edfHY3Wmc&id=1000&type=article" target="_blank">Learn More</a>',
            'Option for Variation2': 'Add variants to a product that has more than one option. You can select from the provided variant types or create your own. <a href="https://university.lazada.co.th/course/learn?spm=a1zawg.20777315.0.0.37894edfHY3Wmc&id=1000&type=article" target="_blank">Learn More</a>',
            'Package Weight (kg)': 'Please ensure you have entered the right package weight (kg) and dimensions (cm) to avoid incorrect shipping fee charges',
            'Stock': 'Input your product stock',
            'Price': 'Input your product price in your local currency.',
            'SellerSKU': 'SKU is a unique identifier for each product variation. SKU value cannot be duplicated in a store.',
            'Package Length (cm)': 'Please ensure you have entered the right package weight (kg) and dimensions (cm) to avoid incorrect shipping fee charges',
            'Package Width (cm)': 'Please ensure you have entered the right package weight (kg) and dimensions (cm) to avoid incorrect shipping fee charges',
            'Package Height (cm)': 'Please ensure you have entered the right package weight (kg) and dimensions (cm) to avoid incorrect shipping fee charges',
        }
    )
    
    result_list.append(
        {
            'Group No': 'You are advised to put a group number for different products, and only input same in subsequent order for products you want to group/are variations.',
            'Category': '*please choose from the drop-down box, or input the correct category id.',
            'Product Name': '*Please input 5 to 255 characters for product name. Only titles in muti-language can be deleted using Overwrite feature during Bulk Edit.',
            'Product Name in EN': 'Please input 5 to 255 characters for product name. Only titles in muti-language can be deleted using Overwrite feature during Bulk Edit.',
            'Product Images1': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images2': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images3': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images4': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images5': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images6': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images7': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Product Images8': '*This is the main image of your product page. Maximum 8 images with size between 330x330 and 5000x5000 px. Max file size: 3 MB. Obscene image is strictly prohibited. Excess images can only be deleted using Overwrite feature during Bulk Edit.',
            'Brand': '*Choose from the ASC, or input directly',
            'Long Description': 'Please input 20 to 3000 characters for product description. Value can only be deleted using Overwrite feature during Bulk Edit. Description with Lorikeet can be edited or deleted on Web. ',
            'Dangerous Goods': 'Select from dropdown list to update. Delete the value using Overwrite feature during Bulk Edit will be given a default value "Yes" if applicable.',
            'Variation Name1': '',
            'Option for Variation1': '',
            'Image per Variation': '',
            'Variation Name2': '',
            'Option for Variation2': '',
            'Package Weight (kg)': '*tr(weight@excel.req2)',
            'Stock': '*Only positive numbers are accepted.',
            'Price': '*Only positive numbers are accepted.',
            'SellerSKU': 'Please input less than 200 characters. Using Overwrite feature to delete during Bulk Edit will be given a random value by system.',
            'Package Length (cm)': '*tr(weight@excel.req2)',
            'Package Width (cm)': '*tr(weight@excel.req2)',
            'Package Height (cm)': '*tr(weight@excel.req2)',
        }
    )

    urls_cnt = len(urls_list)
    cnt = 1
    for url in urls_list:
        response = requests.get(url=url, headers=headers)
        soup = bs(response.text, 'lxml')

        name_eng = ''
        try:
            item_name1 = soup.find(
                'span', class_='pip-header-section__description-text').text.strip().replace(',', '')
            item_name2 = soup.find(
                'span', class_='pip-header-section__title--big notranslate').text.strip()
            name_thai = item_name1 + ' ' + item_name2
            name_en_lst = name_thai.split(' ')
            for s in name_en_lst:
                if s != s.upper():
                    name_eng += s + ' '
                else:
                    s_th_en = translator_th.translate(s)
                    name_eng += s_th_en + ' '
        except Exception as _ex:
            item_name1 = None

        try:
            item_artcl = soup.find(
                'span', class_='pip-product-identifier__value').text.strip()
            item_artcl = item_artcl.replace('.', '')
        except Exception as _ex:
            item_artcl = None

        try:
            item_price = soup.find_all(
                'span', class_='pip-temp-price__integer')
            price = item_price[-1].text.strip().replace(',', '')
            full_price = round(int(price) * 1.2)
        except Exception as _ex:
            item_price = None

        category_lst = []
        try:
            item_category = soup.find_all(
                'li', class_='bc-breadcrumb__list-item')  # .text.strip()
            for category in item_category:
                category = category.text.strip()
                category_lst.append(category)
            clear_category = category_lst[-1]
        except Exception as _ex:
            item_category = None

        try:
            item_material = soup.find(
                'dl', class_='pip-product-details__section').text.strip()
        except Exception as _ex:
            item_material = ''

        try:
            item_instruction = soup.find(
                'a', class_='pip-product-details__document-link').get('href')
        except Exception as _ex:
            item_instruction = ''

        desc_lst = []
        try:
            item_descriptions = soup.find(
                'div', class_='pip-product-details__container')
            p1 = item_descriptions.find_all(
                'p', class_='pip-product-details__paragraph')
            for p in p1:
                p = p.text.strip()
                desc_lst.append(p)
            span = item_descriptions.find(
                'span', class_='pip-product-details__header').text.strip()
            p2 = item_descriptions.find(
                'p', class_='pip-product-details__label').text.strip()
            desc_lst.append(span)
            desc_lst.append(p2)
            item_description = '\n'.join(desc_lst)
        except Exception as _ex:
            try:
                item_descriptions = soup.find(
                    'div', class_='pip-product-details__container')
                span = item_descriptions.find(
                    'span', class_='pip-product-details__header').text.strip()
                p2 = item_descriptions.find(
                    'p', class_='pip-product-details__label').text.strip()
                desc_lst.append(span)
                desc_lst.append(p2)
                item_description = '\n'.join(desc_lst)
                item_description = item_description + '\nน้ำหนักขนส่ง'
            except Exception as _ex:
                item_description = ''

        size_lst = []
        no_size_lst = []
        try:
            item_nosizes = soup.find('div', class_='pip-product-dimensions__measurement-container').find_all(
                'p', class_='pip-product-dimensions__measurement-wrapper')
            for nosize in item_nosizes:
                nosizes = nosize.text.strip()
                nosize = nosizes.replace('\xa0', '')
                no_size_lst.append(nosize)

            for c in no_size_lst:
                clear_size = ''
                for s in c[:-1]:
                    if s in '0123456789.':
                        clear_size += s
                size_lst.append(clear_size)

            weight = size_lst[-1]
            length = size_lst[2]
            width = size_lst[0]
            height = size_lst[1]
        except Exception as _ex:
            item_nosizes = None

        img_lst = []
        product_images = [''] * 8
        try:
            item_imgs = soup.find('div', class_='pip-product__left-top').find_all(
                'div', class_='js-range-media-grid pip-media-grid')
            for div in item_imgs:
                divs = div.find_all('img')
                for img in divs[:8]:
                    urls = img.get('src')
                    img_lst.append(urls)
            for i in range(len(img_lst)):
                product_images[i] = img_lst[i]
        except Exception as _ex:
            item_imgs = None

        long_desc = item_description + f'\n{item_material}\n{item_instruction}'


        result_list.append(
            {
                'Group No': '',
                'Category': clear_category,
                'Product Name': name_thai,
                'Product Name in EN': name_eng,
                'Product Images1': product_images[0],
                'Product Images2': product_images[1],
                'Product Images3': product_images[2],
                'Product Images4': product_images[3],
                'Product Images5': product_images[4],
                'Product Images6': product_images[5],
                'Product Images7': product_images[6],
                'Product Images8': product_images[7],
                'Brand': 'IKEA',
                'Long Description': long_desc,
                'Dangerous Goods': '',
                'Variation Name1': '',
                'Option for Variation1': '',
                'Image per Variation': '',
                'Variation Name2': '',
                'Option for Variation2': '',
                'Package Weight (kg)': weight,
                'Stock': '100',
                'Price': full_price,
                'SellerSKU': item_artcl,
                'Package Length (cm)': length,
                'Package Width (cm)': width,
                'Package Height (cm)': height,
            }
        )


        time.sleep(random.randrange(2, 5))

        if cnt % 10 == 0:
            time.sleep(random.randrange(5, 9))

        print(f'[+] Processed: {cnt}/{urls_cnt}')

        cnt += 1

    #! запись в xls

    df = pd.DataFrame(result_list)
    df.to_excel('result.xlsx', index=False)

    return '[INFO] Data collected successfully!'


def main():
    print(get_source_html(url=url))
    print(get_item_urls())
    print(get_data())


if __name__ == '__main__':
    main()