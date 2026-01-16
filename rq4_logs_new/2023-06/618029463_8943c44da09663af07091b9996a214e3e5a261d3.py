""" Libor Havránek App Copyright (C)  9.6 2023 """

import unittest

from bs4 import BeautifulSoup
from werkzeug.security import generate_password_hash

from myshop import create_app, db
from myshop.models.customer_model import Customer
from myshop.tests.my_test_mixin import TestAllTemplates, TestMixin


class TestAddMobileProduct(TestAllTemplates):
    """Test edit brand page."""

    path = "/products/edit-smart-watch-product/1"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()


class TestProductTemplateOnlyAddSmartWatchProductTemplate(TestMixin, unittest.TestCase):
    """Test what are specific only for this template"""

    @classmethod
    def setUpClass(cls):
        cls.test_name = cls.__name__

    def setUp(self):
        self.app = create_app(config={"TESTING": True})
        self.app.testing = True
        self.client = self.app.test_client()
        app_context = self.app.app_context()
        app_context.push()
        self.app.config["TESTING"] = True
        self.app.config["WTF_CSRF_ENABLED"] = False
        self.app.secret_key = "test_secret_key"
        super().setUp()

    def login_user(self):
        user_password = "password"
        customer = Customer()
        customer.username = "testuser"
        customer.email = "testuser@example.com"
        customer.user_password = generate_password_hash(user_password, method="sha256")
        db.session.add(customer)
        db.session.commit()
        data = {"email": "testuser@example.com", "password": "password"}
        self.client.post("/auth/login", data=data, follow_redirects=True)

    def test_add_product_form_have_closed_form_tag(self):
        self.login_user()
        response = self.client.get(
            "/products/edit-smart-watch-product/1", follow_redirects=True
        )
        self.assertIn(
            b'<form method="POST" autocomplete="off" enctype=multipart/form-data>',
            response.data,
        )
        self.assertIn(b"</form>", response.data)

    def test_product_form_have_all_input_fields(self):
        self.login_user()
        fields_to_test = [
            "csrf_token",
            "product_name",
            "price",
            "discount",
            "display_size",
            "memory",
            "battery_capacity",
            "weight",
            "wifi",
            "bluetooth",
            "nfc",
            "esim",
            "heart_rate_monitor",
            "step_counter",
            "sleep_tracker",
            "gps",
            "water_resistant",
            "music_player",
            "voice_assistant",
            "edit_product_submit",
        ]

        response = self.client.get(
            "/products/edit-smart-watch-product/1", follow_redirects=True
        )
        soup = BeautifulSoup(response.data, "html.parser")
        form_tag = soup.find("form", {"method": "POST"})
        form_input_fields = [
            input_tag["name"] for input_tag in form_tag.find_all("input")
        ]

        for field in fields_to_test:
            with self.subTest(field=field):
                self.assertIn(field, form_input_fields)

    def test_product_form_have_all_select_fields(self):
        self.login_user()
        fields_to_test = [
            "color",
            "brand_id",
            "category_id",
            "display_resolution",
            "operating_system",
            "weight_units",
        ]

        response = self.client.get(
            "/products/edit-smart-watch-product/1", follow_redirects=True
        )
        soup = BeautifulSoup(response.data, "html.parser")
        form_tag = soup.find("form", {"method": "POST"})
        form_select_fields = [
            select_tag["name"] for select_tag in form_tag.find_all("select")
        ]

        for field in fields_to_test:
            with self.subTest(field=field):
                self.assertIn(field, form_select_fields)

    def test_product_form_have_all_text_area_fields(self):
        self.login_user()
        fields_to_test = ["subheading", "description"]

        response = self.client.get(
            "/products/edit-smart-watch-product/1", follow_redirects=True
        )
        soup = BeautifulSoup(response.data, "html.parser")
        form_tag = soup.find("form", {"method": "POST"})
        form_select_fields = [
            select_tag["name"] for select_tag in form_tag.find_all("textarea")
        ]

        for field in fields_to_test:
            with self.subTest(field=field):
                self.assertIn(field, form_select_fields)

    def test_add_product_form_have_all_labels(self):
        self.login_user()
        expected_labels = {
            "product_name": "Název *:",
            "price": "Cena *:",
            "discount": "Sleva:",
            "description": "Popis *:",
            "subheading": "Podnadpis *:",
            "display_size": "Velikost displeje *:",
            "display_resolution": "Rozlišení displeje *:",
            "operating_system": "Operační systém *:",
            "memory": "Velikost disku *:",
            "battery_capacity": "Kapacita baterie:",
            "weight": "Váha:",
            "color": "Barva:",
            "wifi": "WiFi:",
            "bluetooth": "Bluetooth:",
            "nfc": "NFC:",
            "esim": "eSIM:",
            "heart_rate_monitor": "Měřič srdečního tepu:",
            "step_counter": "Počítadlo kroků:",
            "sleep_tracker": "Sledovač spánku:",
            "gps": "GPS:",
            "water_resistant": "Voděodolné:",
            "music_player": "Přehrávač hudby:",
            "voice_assistant": "Hlasový asistent:",
            "brand_id": "Značka:",
            "category_id": "Kategorie:",
        }
        response = self.client.get(
            "/products/edit-smart-watch-product/1", follow_redirects=True
        )
        soup = BeautifulSoup(response.data, "html.parser")
        form_labels = {
            label_tag["for"]: label_tag.text.strip()
            for label_tag in soup.find_all("label")
        }

        for field, label in expected_labels.items():
            with self.subTest(field=field):
                self.assertIn(label, form_labels[field])

    def test_add_product_form_have_submit_field(self):
        self.login_user()
        response = self.client.get(
            "/products/edit-smart-watch-product/1", follow_redirects=True
        )
        soup = BeautifulSoup(response.data, "html.parser")

        # Check that the form contains a submit button
        submit_button = soup.find(
            "input", {"type": "submit", "value": "Upravit produkt"}
        )
        self.assertIsNotNone(submit_button)