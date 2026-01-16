import requests

class Openfoodfacts:

    def __init__(self):
        self.products = []

    def search_product(self, terms):
        products_list = []
        products = []

        for term in terms:
            payload = {
                'search_terms': term,
                'sort_by': 'unique_scans_n',
                'page_size': 10,
                'json': 1
            }

            res = requests.get('https://world.openfoodfacts.org/cgi/search.pl?', 
                            params=payload)
            results = res.json()
            products_list.append(results)

        for prod in products_list:
            product = prod['products']
            products.append(product)
        
        self.products = products