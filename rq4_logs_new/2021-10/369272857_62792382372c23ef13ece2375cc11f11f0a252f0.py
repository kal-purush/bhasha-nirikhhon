import requests
from bs4 import BeautifulSoup

from writters import TXTWriter, CSVWriter, DBWriter, JSONWriter

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit 537.36'
                         '(KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
          }

ROOT = 'https://www.work.ua'

full_url = ROOT + '/ru/jobs/'

# result = []
page = 0

writers_list = [
    TXTWriter(),
    CSVWriter(),
    DBWriter(),
    JSONWriter(),
]


def get_replace(item: str):
    dict_vac = {
        "\u202f": "",
        "\u2009": "",
        "грн": "",
        "'": "`",
    }
    for i, j in dict_vac.items():
        item = item.replace(i, j)
    return item


def get_salary(card_table):
    try:
        salary = card_table.find('b', class_="text-black").get_text(strip=True)
        salary = get_replace(salary).strip()
    except AttributeError:
        salary = "no data available"
    return salary


def get_detail_vac(root_url: str, job_card_link: str, headers: dict) -> dict:

    response = requests.get(root_url+job_card_link, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    job_card_table = soup.find('div', class_='card wordwrap')

    salary = get_salary(job_card_table)

    return salary


while True:
    page += 1
    print(f'Page: {page}')

    # TODO
    # if page == 5:
    #     break

    params = {
        'page': page,
    }

    response = requests.get(full_url, headers=HEADERS, params=params)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    job_list_container = soup.find("div", {"id": "pjax-job-list"})

    # no jobs left
    if job_list_container is None:
        break
    # breakpoint()

    jobs = job_list_container.find_all('div', {'class': 'card card-hover card-visited wordwrap job-link js-hot-block'})

    for job in jobs:
        href = job.find('a')['href']
        id_ = ''.join(char for char in href if char.isdigit())
        title = job.find('a').text
        detail_vacancy = get_detail_vac(ROOT, href, HEADERS)
        job_info = {
            'id': id_,
            'href': href,
            'title': title,
            'salary': detail_vacancy,

        }

        for writer in writers_list:
            writer.write(job_info)
