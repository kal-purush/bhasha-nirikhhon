import unittest

from werkzeug.security import generate_password_hash

from myshop import create_app, db
from myshop.models.customer_model import Customer
from myshop.tests.my_test_mixin import TestMixin


class TestAddConsole(TestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_name = cls.__name__

    def setUp(self):
        self.app = create_app(config={'TESTING': True})
        self.app.testing = True
        self.client = self.app.test_client()
        app_context = self.app.app_context()
        app_context.push()
        self.app.config['TESTING'] = True
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.app.secret_key = 'test_secret_key'
        super().setUp()

    def login_user(self):
        user_password = "password"
        customer = Customer()
        customer.username = "testuser"
        customer.email = "testuser@example.com"
        customer.user_password = generate_password_hash(user_password, method='sha256')
        db.session.add(customer)
        db.session.commit()
        data = {
            "email": "testuser@example.com",
            "password": "password"
        }
        self.client.post('/auth/login', data=data, follow_redirects=True)

    def create_product(self):
        self.login_user()

        brand_data = {
            "brand_name": "Sony",
        }
        self.client.post('/products/create-brand', data=brand_data, follow_redirects=True)

        category_data = {
            "category_name": "Console",
        }
        self.client.post('/products/create-category', data=category_data, follow_redirects=True)

        product_data = {
            "product_name": "Xbox Series X 100GB",
            "price": 13999,
            "discount": 5,
            "stock": 20,
            "description": "Xbox Series X je nejnovější generace herní konzole, která vám přináší úžasný herní zážitek. "
                           "S výkonným hardwarovým vybavením a podporou ray tracingu si užijete realistické a plynulé hraní"
                           " s nádhernou grafikou. Díky rychlému načítání se okamžitě dostanete do hry a můžete si "
                           "užívat bez prodlev. Xbox Series X je vybavený technologií Spatial Sound, která vám poskytuje "
                           "výjimečný prostorový zvuk a ponoří vás přímo do děje hry. S novým bezdrátovým ovladačem si "
                           "užijete přesnou a pohodlnou kontrolu nad hrou. Xbox Series X je připravený rozšířit"
                           " vaše hraní na novou úroveň.",
            "subheading": "Připravte se na úžasný herní zážitek s Xbox Series X.",
            "visit_count": 5421,
            "product_type": "Herní konzole",
            "brand_id": 1,
            "category_id": 1,
            "color": "černá",
            "product_image": "test_image_xbox_series_x.jpeg",
            "ssd_capacity": 1024,
            "hdd_capacity": 0,
            "ssd": True,
            "hdd": False,
            "dvd_drive": True
        }

        self.client.post('/products/create-console-product', data=product_data, follow_redirects=True)



    def test_create_product_return_correct_message_when_short_name(self):
        self.login_user()
        data = {
            "product_name": "X",
            "price": 13999,
            "discount": 5,
            "stock": 20,
            "description": "Xbox Series X je nejnovější generace herní konzole, která vám přináší úžasný herní zážitek. "
                           "S výkonným hardwarovým vybavením a podporou ray tracingu si užijete realistické a plynulé hraní"
                           " s nádhernou grafikou. Díky rychlému načítání se okamžitě dostanete do hry a můžete si "
                           "užívat bez prodlev. Xbox Series X je vybavený technologií Spatial Sound, která vám poskytuje "
                           "výjimečný prostorový zvuk a ponoří vás přímo do děje hry. S novým bezdrátovým ovladačem si "
                           "užijete přesnou a pohodlnou kontrolu nad hrou. Xbox Series X je připravený rozšířit"
                           " vaše hraní na novou úroveň.",
            "subheading": "Připravte se na úžasný herní zážitek s Xbox Series X.",
            "visit_count": 5421,
            "product_type": "Herní konzole",
            "brand_id": 10,
            "category_id": 8,
            "color": "černá",
            "product_image": "test_image_xbox_series_x.jpeg",
            "ssd_capacity": 1024,
            "hdd_capacity": 0,
            "ssd": True,
            "hdd": False,
            "dvd_drive": True
        }
        response = self.client.post('/products/create-console-product', data=data, follow_redirects=True)
        self.assertIn(bytes("Produkt musí mít alespoň dva znaky.", "utf-8"), response.data)

    def test_create_product_return_correct_message_when_price_is_zero(self):
        self.login_user()
        data = {
                        "product_name": "Xbox Series X 100GB",
            "price": 0,
            "discount": 5,
            "stock": 20,
            "description": "Xbox Series X je nejnovější generace herní konzole, která vám přináší úžasný herní zážitek. "
                           "S výkonným hardwarovým vybavením a podporou ray tracingu si užijete realistické a plynulé hraní"
                           " s nádhernou grafikou. Díky rychlému načítání se okamžitě dostanete do hry a můžete si "
                           "užívat bez prodlev. Xbox Series X je vybavený technologií Spatial Sound, která vám poskytuje "
                           "výjimečný prostorový zvuk a ponoří vás přímo do děje hry. S novým bezdrátovým ovladačem si "
                           "užijete přesnou a pohodlnou kontrolu nad hrou. Xbox Series X je připravený rozšířit"
                           " vaše hraní na novou úroveň.",
            "subheading": "Připravte se na úžasný herní zážitek s Xbox Series X.",
            "visit_count": 5421,
            "product_type": "Herní konzole",
            "brand_id": 10,
            "category_id": 8,
            "color": "černá",
            "product_image": "test_image_xbox_series_x.jpeg",
            "ssd_capacity": 1024,
            "hdd_capacity": 0,
            "ssd": True,
            "hdd": False,
            "dvd_drive": True
        }
        response = self.client.post('/products/create-console-product', data=data, follow_redirects=True)
        self.assertIn(bytes("Cena produktu nemůže být nulová.", "utf-8"), response.data)

    def test_create_product_return_correct_message_when_description_is_short(self):
        self.login_user()
        data = {
            "product_name": "Xbox Series X 100GB",
            "price": 13999,
            "discount": 5,
            "stock": 20,
            "description": "Xbox Series X",
            "subheading": "Připravte se na úžasný herní zážitek s Xbox Series X.",
            "visit_count": 5421,
            "product_type": "Herní konzole",
            "brand_id": 10,
            "category_id": 8,
            "color": "černá",
            "product_image": "test_image_xbox_series_x.jpeg",
            "ssd_capacity": 1024,
            "hdd_capacity": 0,
            "ssd": True,
            "hdd": False,
            "dvd_drive": True
        }
        response = self.client.post('/products/create-console-product', data=data, follow_redirects=True)
        self.assertIn(bytes("Popis musí mít alespoň padesát znaků.", "utf-8"), response.data)

    def test_create_product_return_correct_message_when_subheading_is_short(self):
        self.login_user()
        data = {
            "product_name": "Xbox Series X 100GB",
            "price": 13999,
            "discount": 5,
            "stock": 20,
            "description": "Xbox Series X je nejnovější generace herní konzole, která vám přináší úžasný herní zážitek. "
                           "S výkonným hardwarovým vybavením a podporou ray tracingu si užijete realistické a plynulé hraní"
                           " s nádhernou grafikou. Díky rychlému načítání se okamžitě dostanete do hry a můžete si "
                           "užívat bez prodlev. Xbox Series X je vybavený technologií Spatial Sound, která vám poskytuje "
                           "výjimečný prostorový zvuk a ponoří vás přímo do děje hry. S novým bezdrátovým ovladačem si "
                           "užijete přesnou a pohodlnou kontrolu nad hrou. Xbox Series X je připravený rozšířit"
                           " vaše hraní na novou úroveň.",
            "subheading": "Připravt",
            "visit_count": 5421,
            "product_type": "Herní konzole",
            "brand_id": 10,
            "category_id": 8,
            "color": "černá",
            "product_image": "test_image_xbox_series_x.jpeg",
            "ssd_capacity": 1024,
            "hdd_capacity": 0,
            "ssd": True,
            "hdd": False,
            "dvd_drive": True
        }
        response = self.client.post('/products/create-console-product', data=data, follow_redirects=True)
        self.assertIn(bytes("Podnadpis musí mít alespoň dvacet znaků.", "utf-8"), response.data)