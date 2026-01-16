""" Libor Havránek App Copyright (C)  9.6 2023 """

import unittest
from _decimal import Decimal

from myshop import create_app, db
from myshop.models.console_model import Console

from myshop.tests.my_test_mixin import TestMixin


class TestConsoleModel(TestMixin, unittest.TestCase):
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

    def create_product(self):
        self.product = Console()
        self.product.product_name = 'Play station 4'
        self.product.price = 999.9
        self.product.discount = 10
        self.product.stock = 100
        self.product.description = "Play station 4: Powerful and sleek console with high-resolution display, " \
                                   "fast processor, and ample storage."
        self.product.subheading = "Play station 4: Powerful and sleek"

        self.product.ssd_capacity = 512
        self.product.hdd_capacity = 1_000
        self.product.ssd = True
        self.product.hdd = True

        self.product.dvd_drive = True

        self.product.color = 'Silver'

        self.product.brand_id = 1
        self.product.category_id = 2
        self.product.product_image = 'product_image_00002.jpg'

        db.session.add(self.product)
        db.session.commit()

    def test_product_have_product_name(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.product_name, 'Play station 4')

    def test_product_have_price(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.price, Decimal('999.90'))

    def test_product_have_discount(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.discount, 10)

    def test_product_have_stock(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.stock, 100)

    def test_product_have_sold(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.sold, 0)

    def test_product_have_description(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.description,
                         "Play station 4: Powerful and sleek console with high-resolution display, "
                         "fast processor, and ample storage.")

    def test_product_have_subheading(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.subheading, "Play station 4: Powerful and sleek")

    def test_product_have_date_created(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertIsNotNone(result.date_created)

    def test_product_have_date_edited(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertIsNone(result.date_edited)

    def test_product_have_edited(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertFalse(result.edited)

    def test_product_have_visit_count(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.visit_count, 0)

    def test_product_have_brand_id(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.brand_id, 1)

    def test_product_have_category_id(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.category_id, 2)

    def test_product_have_product_image(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.product_image, 'product_image_00002.jpg')

    def test_product_have_images(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(len(result.images), 0)

    def test_product_have_ssd_capacity(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.ssd_capacity, 512)

    def test_product_have_hdd_capacity(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertEqual(result.hdd_capacity, 1000)

    def test_product_have_ssd(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertTrue(result.ssd)

    def test_product_have_hdd(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertTrue(result.hdd)

    def test_product_have_cd_dvd_drive(self):
        self.create_product()
        result = Console.query.filter_by(id=1).first()
        self.assertTrue(result.dvd_drive)