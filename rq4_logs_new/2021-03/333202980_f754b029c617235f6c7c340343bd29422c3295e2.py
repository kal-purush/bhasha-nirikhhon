import requests
import csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import click


def get_soup(response):
    return BeautifulSoup(response.text, 'lxml')


def get_href_list(soup, need_tag1, need_tag_in_need_tag1, need_param_in_need_tag1):
    return soup.find_all(need_tag1, attrs={need_tag_in_need_tag1: need_param_in_need_tag1})


def get_link_job_vacancy(base_url, job_url):
    list_link = []
    for item in job_url:
        list_link.append(urljoin(base_url, item.attrs['href'] + '/'))
    return list_link


def process_offer(writer, soup_data, url):
    name_tag = soup_data.find('h1')
    name = name_tag.text.strip()

    company_tag = soup_data.find('h2')
    company = company_tag.text.strip()

    inform_tag = soup_data.find_all('div', class_='vacancy-ssr-contact-item')
    address, contact_person, contact_telephone = None, None, None
    for inform in inform_tag:
        if 'Адрес' in inform.find_all('span')[0].text:
            address = inform.find_all('span')[1].text
        if 'Контакт' in inform.find_all('span')[0].text:
            contact_person = inform.find_all('span')[1].text
        if 'Телефон' in inform.find_all('span')[0].text:
            contact_telephone = inform.find_all('span')[1].text
    
    writer.writerow({
    'Job Title': name,
    'Name of the company': company,
    'Address': address,
    'Contact': contact_person,
    'Telephone': contact_telephone,
    'URL': url
})




@click.command()
@click.argument('sait', type=str)
def main(sait):
    BASE_URL = f'https://{sait}/'
    SEARCH_QUERY = 'zapros/junior-python-developer'

    response = requests.get(urljoin(BASE_URL, SEARCH_QUERY + '/'))
    soup = get_soup(response)

    href_job = get_href_list(soup, 'a', 'class', 'ga_listing')
    link_job = get_link_job_vacancy(BASE_URL, href_job)
    
    filename = 'junior-python-developer.csv'
    with open(filename, mode='w') as f:
        header = ['Job Title', 'Name of the company', 'Address', 'Contact', 'Telephone', 'URL']
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()

        with click.progressbar(link_job, label='Process offers') as bar:
            for item in bar:
                response_link_job = requests.get(item)
                data_link_job = get_soup(response_link_job)
                process_offer(writer, data_link_job, item)


if __name__ == '__main__':
    main()