import requests
from bs4 import BeautifulSoup


class Parser:
    html = ""
    res = []

    def __init__(self, url, path):
        self.url = url
        self.path = path

    def get_html(self):
        req = requests.get(self.url).text
        self.html = BeautifulSoup(req, 'lxml')

    def parsing(self):
        p1 = self.html.find("section", id="catalog-content")
        ob = p1.find_all('article', class_="_card_8sstg_1 _card--autoheight-mobile_8sstg_388")
        for plugin in ob:
            name = plugin.find("section", class_="_card__content_8sstg_390").find("a").text
            opr = plugin.find("span", class_="_card__subtitle_8sstg_1").text
            avto = plugin.find("a", class_="_card__author_8sstg_171").find("span").text
            numb = plugin.find("section", class_="_card__info_8sstg_1").text.strip()
            self.res.append({
                'name': name,
                'opr': opr,
                'avto': avto,
                'numb': numb
            })
        # print(self.res)

    def save(self):
        with open(self.path, 'w') as f:
            i = 1
            for item in self.res:
                f.write(
                    f"Новость № {i}\n\nНазвание: {item['name']}\nСсылка: {item['opr']}\nАвтор: {item['avto']}"
                    f"\nДата/Отзывы:{item['numb']}\n"
                    f"\n\n{'*' * 20}\n")
                i += 1

    def run(self):
        self.get_html()
        self.parsing()
        self.save()