""" Libor Havránek App Copyright (C)  9.6 2023 """

import unittest
from _decimal import Decimal

from myshop import create_app, db
from myshop.models.monitor_model import Monitor

from myshop.tests.my_test_mixin import TestMixin


class TestMonitorModel(TestMixin, unittest.TestCase):
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
        self.product = Monitor()
        self.product.product_name = 'Samsung smart M7'
        self.product.price = 999.9
        self.product.discount = 10
        self.product.stock = 100
        self.product.description = "Samsung smart M7: Powerful and sleek monitor with high-resolution display, " \
                                   "fast processor, and ample storage."
        self.product.subheading = "Samsung smart M7: Powerful and sleek"

        self.product.color = 'Silver'

        self.product.display_size = 27.0
        self.product.display_resolution = '2560x1440'
        self.product.refresh_rate = 144
        self.product.response_time = 1
        self.product.aspect_ratio = '16:9'
        self.product.connectivity = 'HDMI, DisplayPort, USB'
        self.product.color_depth = 8
        self.product.curvature = False
        self.product.height = 45.0
        self.product.height_units = 'cm'
        self.product.width = 61.0
        self.product.width_units = 'cm'
        self.product.depth = 18.0
        self.product.depth_units = 'cm'
        self.product.weight = 5.5
        self.product.weight_units = 'kg'
        self.product.color = 'Silver'
        self.product.adjustable_stand = True
        self.product.wall_mountable = False
        self.product.built_in_speakers = True
        self.product.energy_efficiency = 'A+'

        self.product.brand_id = 1
        self.product.category_id = 2
        self.product.product_image = 'product_image_00002.jpg'

        db.session.add(self.product)
        db.session.commit()

    def test_product_have_product_name(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.product_name, 'Samsung smart M7')

    def test_product_have_price(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.price, Decimal('999.90'))

    def test_product_have_discount(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.discount, 10)

    def test_product_have_stock(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.stock, 100)

    def test_product_have_sold(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.sold, 0)

    def test_product_have_description(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.description,
                         "Samsung smart M7: Powerful and sleek monitor with high-resolution display, "
                         "fast processor, and ample storage.")

    def test_product_have_subheading(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.subheading, "Samsung smart M7: Powerful and sleek")

    def test_product_have_date_created(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertIsNotNone(result.date_created)

    def test_product_have_date_edited(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertIsNone(result.date_edited)

    def test_product_have_edited(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertFalse(result.edited)

    def test_product_have_visit_count(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.visit_count, 0)

    def test_product_have_brand_id(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.brand_id, 1)

    def test_product_have_category_id(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.category_id, 2)

    def test_product_have_product_image(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.product_image, 'product_image_00002.jpg')

    def test_product_have_images(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(len(result.images), 0)

    def test_product_have_display_size(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.display_size, 27.0)

    def test_product_have_display_resolution(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.display_resolution, '2560x1440')

    def test_product_have_refresh_rate(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.refresh_rate, 144)

    def test_product_have_response_time(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.response_time, 1)

    def test_product_have_aspect_ratio(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.aspect_ratio, '16:9')

    def test_product_have_connectivity(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.connectivity, 'HDMI, DisplayPort, USB')

    def test_product_have_color_depth(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.color_depth, 8)

    def test_product_have_curvature(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.curvature, False)

    def test_product_have_height(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.height, 45.0)

    def test_product_have_height_units(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.height_units, 'cm')

    def test_product_have_width(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.width, 61.0)

    def test_product_have_width_units(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.width_units, 'cm')

    def test_product_have_depth(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.depth, 18.0)

    def test_product_have_depth_units(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.depth_units, 'cm')

    def test_product_have_weight(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.weight, 5.5)

    def test_product_have_weight_units(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.weight_units, 'kg')

    def test_product_have_adjustable_stand(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.adjustable_stand, True)

    def test_product_have_wall_mountable(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.wall_mountable, False)

    def test_product_have_built_in_speakers(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.built_in_speakers, True)

    def test_product_have_energy_efficiency(self):
        self.create_product()
        result = Monitor.query.filter_by(id=1).first()
        self.assertEqual(result.energy_efficiency, 'A+')