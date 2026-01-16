
import requests as r
import difflib

base_url = "https://api.thecatapi.com/v1/images/search"
CATegories = "https://api.thecatapi.com/v1/categories"
breeds = "https://api.thecatapi.com/v1/breeds"


class catdata:

    def __init__(self):
        self.base_url = base_url
        self.CATegories = CATegories
        self.breeds = breeds
        self.tag = None
        self.sample = None



    def check_cate(self, sample="None"):
        page = r.get(CATegories)
        pagelist = page.json()
        names = []
        sample = sample.lower()
        for category in pagelist:
            name = category.get('name')
            names.append(name)

        result = difflib.get_close_matches(sample, names)
        for category1 in pagelist:
            if category1.get('name') == str(result)[2:-2]:
                return category1.get('id')
            else:
                continue


    def check_breed(self, sample="None"):
        sample = sample.lower()
        names = []
        page = r.get(breeds)
        pagelist = page.json()
        for cats in pagelist:
            names.append(cats.get('name'))
        result = difflib.get_close_matches(sample, names)
        for cat1 in pagelist:
            if cat1.get('name') == str(result)[2:-2]:
                return cat1.get('id')
            else:
                continue

    def specify(self, query=None):

        cate = self.check_cate(query)
        breed = self.check_breed(query)

        if cate or breed is not None:
            if cate is not None:
                url = f"https://api.thecatapi.com/v1/images/search?category_ids={cate}"
            else:
                pass
            if breed is not None:
                url = f"https://api.thecatapi.com/v1/images/search?breed_ids={breed}"
            else:
                pass
        else:
            url = None

        return url

    def getfromurl(self, tag):
        url = self.specify(tag)
        page = r.get(url)
        page_dict = page.json()[0]
        return page_dict

    def wiki(self, tag):
        d = self.getfromurl(tag)
        return d.get('wikipedia_url')