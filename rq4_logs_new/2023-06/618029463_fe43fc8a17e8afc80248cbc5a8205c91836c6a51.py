""" Libor Havránek App Copyright (C)  8.6. 2023 """
import unittest

from myshop import create_app
from myshop.forms.add_console_form import ConsoleForm
from myshop.tests.my_test_mixin import TestMixin


class TestConsoleForm(TestMixin, unittest.TestCase):
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
        self.form = ConsoleForm()
        super().setUp()

    def test_product_form_have_csrf_token_field(self):
        self.assertIn("csrf_token", self.form._fields)

    def test_product_form_have_product_name_field(self):
        self.assertIn("product_name", self.form._fields)

    def test_product_form_have_price_field(self):
        self.assertIn("price", self.form._fields)

    def test_product_form_have_discount_field(self):
        self.assertIn("discount", self.form._fields)

    def test_product_form_have_stock_field(self):
        self.assertIn("stock", self.form._fields)

    def test_product_form_have_description_field(self):
        self.assertIn("description", self.form._fields)

    def test_product_form_have_subheading_field(self):
        self.assertIn("subheading", self.form._fields)

    def test_product_form_have_color_field(self):
        self.assertIn("color", self.form._fields)

    def test_product_form_have_ssd_field(self):
        self.assertIn("ssd", self.form._fields)

    def test_product_form_have_hdd_field(self):
        self.assertIn("hdd", self.form._fields)

    def test_product_form_have_ssd_capacity_field(self):
        self.assertIn("ssd_capacity", self.form._fields)

    def test_product_form_have_hdd_capacity_field(self):
        self.assertIn("hdd_capacity", self.form._fields)

    def test_product_form_have_dvd_drive_field(self):
        self.assertIn("dvd_drive", self.form._fields)

    def test_product_form_have_brand_id_field(self):
        self.assertIn("brand_id", self.form._fields)

    def test_product_form_have_category_id_field(self):
        self.assertIn("category_id", self.form._fields)

    def test_product_form_have_product_image_field(self):
        self.assertIn("product_image", self.form._fields)

    def test_product_form_have_additional_images_field(self):
        self.assertIn("additional_images", self.form._fields)

    def test_product_form_have_add_product_submit_field(self):
        self.assertIn("add_product_submit", self.form._fields)

    def test_product_form_have_edit_product_submit_field(self):
        self.assertIn("edit_product_submit", self.form._fields)