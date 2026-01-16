""" Libor Havránek App Copyright (C)  8.6. 2023 """

import unittest

from wtforms.validators import InputRequired

from myshop import create_app
from myshop.forms.edit_all_product_image import AddProductAdditionalImagesForm
from myshop.forms.edit_product_image import EditProductMainImageForm


class TestAddProductAdditionalImagesForm(unittest.TestCase):
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
        self.form = AddProductAdditionalImagesForm()
        super().setUp()

    def test_add_product_additional_images_form_exists(self):
        self.assertIsNotNone(self.form)

    def test_additional_images_field_exists(self):
        self.assertIn("additional_images", self.form._fields)

    def test_add_product_additional_image_submit_field_exists(self):
        self.assertIn("add_product_additional_image_submit", self.form._fields)

    def test_add_product_additional_image_submit_exists(self):
        field = self.form.add_product_additional_image_submit
        self.assertIn(field.name, self.form._fields)


class TestEditProductMainImageForm(unittest.TestCase):
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
        self.form = EditProductMainImageForm()
        super().setUp()

    def test_edit_product_main_image_form_exists(self):
        self.assertIsNotNone(self.form)

    def test_product_image_field_exists(self):
        self.assertIn("product_image", self.form._fields)

    def test_edit_product_main_image_submit_field_exists(self):
        self.assertIn("edit_product_main_image_submit", self.form._fields)

    def test_product_image_required(self):
        field = self.form.product_image
        self.assertTrue(isinstance(field.validators[0], InputRequired))

    def test_edit_product_main_image_submit_exists(self):
        field = self.form.edit_product_main_image_submit
        self.assertIn(field.name, self.form._fields)