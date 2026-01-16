import mysql.connector
from mysql.connector import errorcode

import requests


class Create_tables:
    """Create tables for database"""

    def __init__(self):
        self.cursor = None

    def connect(self, host, user, password):
        """Connect to mysql server"""
        cnx = mysql.connector.connect(host=host, user=user, password=password)
        self.cursor = cnx.cursor()

    def use_db(self, database):
        """To use a database"""
        self.cursor.execute('USE {}'.format(database))

    def creating_tables(self, tables):
        """Creating tables"""
        for table in tables:
            try:
                self.cursor.execute(table)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)


class Get_datas:
    """Get datas from openfoodfacts API"""

    def __init__(self, term, category):
        self.payload = {
            'search_terms': term,
            'search_tag': category,
            'sort_by': 'unique_scans_n',
            'page_size': 10,
            'json': 1
        }
        self.products = None
        self.product_name = []
        self.nutriscores = []
        self.categories = []
        self.links = []
        self.stores = []
        self.details = []
        self.dictionnary_datas = []

    def get_json(self):
        """Get datas in json format from openfoodfacts API"""
        res = requests.get('https://world.openfoodfacts.org/cgi/search.pl?', params=self.payload)
        results = res.json()
        products_list = results['products']

        self.products = products_list

    def get_attribute(self, attribute_name, list_to_fill):
        """Get attribute we need from openfoodfacts API"""
        for product in self.products:
            try:
                attribute = product[attribute_name]
            except KeyError:
                attribute = 0

            list_to_fill.append(attribute)

    def set_dictionnary(self):
        """Create dictionnary to insert datas into the db"""
        i = 0
        for item in self.product_name:
            line = {
                'product_name': item,
                'nutriscore': self.nutriscores[i],
                'link': self.links[i],
                'details': self.details[i],
                'categories': self.categories[i],
                'stores': self.stores[i]
            }

            self.dictionnary_datas.append(line)

            i += 1