import csv
import requests
from multiprocessing import Pool
from time import sleep


def get_html(url):  # chtob ne zabanil nas server mozhno ispol'zovat funkciyu sleep ...
    sleep(1)  # mezhdu zaprosami budet 1 sekunda , no mozhna i bez sleep , prosto ego udalit' ...
    r = requests.get(url)
    return r.text


def write_csv(data):
    with open('websites.csv', 'a') as f:
        order = ['name', 'url', 'description', 'traffic', 'percent']
        writer = csv.DictWriter(f, fieldnames=order)
        writer.writerow(data)


# Parsim tekst a ne html
def get_page_data(text):
        data = text.strip().split('\n')[1:]  # data vernet spisok !!!

        for r in data:  # kazhdaya r eto stroka
            columns = r.strip().split('\t')
            name = columns[0]
            url = columns[1]
            description = columns[2]
            traffic = columns[3]
            percent = columns[4]

            data = {'name': name,
                    'url': url,
                    'description': description,
                    'traffic': traffic,
                    'percent': percent
                    }
            write_csv(data)


def make_all(url):
    text = get_html(url)
    get_page_data(text)


def main():
    url = 'https://www.liveinternet.ru/rating/ru//today.tsv?page={}'
    urls = [url.format(str(i)) for i in range(1, 6321)]  # Kolichestvo stranic na liveinternet.ru

    with Pool(20) as p:  # Pool eto klas vsh processov a 20 eto kolichestvo processov
        p.map(make_all, urls)  # p.map beret kazhdiy url i k nemu primenyaet funkciyu make_all


if __name__ == '__main__':
    main()

"""
Multi parsing ne goditsa dlya slabih serverov ,
esli multiparsing medlenno rabotaet , vozmozhno server umishlenno , 
zamedlyaet process otveta na zaprosi ...
"""