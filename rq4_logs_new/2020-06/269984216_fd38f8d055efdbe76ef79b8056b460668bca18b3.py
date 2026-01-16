import requests
from googlesearch import search
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from selenium import webdriver


def find_doc_for_police_department(police_dept, state):
    # Initialize a headless browser to deal with pages that require javascript rendering
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver = webdriver.Chrome(options=options)

    def scrape_page(url):
        key_words = ['policy', 'policies']
        session = HTMLSession()
        response = session.get(url)

        if '/cms/' in url:
            # Got to a customer management system
            # Example url = 'https://www.yubacity.net/cms/One.aspx?portalId=239258&pageId=16298984'
            # YUBA deleted its policy document??????
            driver.get(url)

            # If it's a folder click on it
            folders = driver.find_elements_by_class_name('item.docTitle')
            for folder in folders:
                folder_title = folder.text.lower()
                if any(key_word in folder_title for key_word in key_words):
                    print(folder.text)
                    folder.click()
                    break

        else:
            soup = BeautifulSoup(response.content, 'lxml')
            additional_pages = []
            anchors = soup.find_all('a')
            for anchor in anchors:
                anchor_url = anchor.attrs.get('href', '')
                anchor_title = anchor.attrs.get('title', '').lower()
                if any(key_word in anchor_title for key_word in key_words):
                    # Confirm this is a valid URL to navigate to
                    if anchor_url.startswith('https'):
                        print(anchor_url)
                        additional_pages.append(anchor_url)

    additional_search_terms = ['police policy manual', 'use of force']
    for additional_search_term in additional_search_terms:
        query = f'{police_dept} {state} {additional_search_term}'
        google_results = search(query, stop=10)
        for google_result in google_results:
            print(google_result)

    driver.close()


def main():
    police_dept = 'yuba city'
    state = 'ca'
    find_doc_for_police_department(police_dept, state)