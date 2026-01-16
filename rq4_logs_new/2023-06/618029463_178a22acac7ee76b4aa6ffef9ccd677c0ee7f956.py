""" Libor Havránek App Copyright (C)  8.6 2023 """

import unittest
from _decimal import Decimal

from myshop import create_app, db
from myshop.models.smart_watch_model import SmartWatch
from myshop.tests.my_test_mixin import TestMixin


class TestCustomerSmartWatchModel(TestMixin, unittest.TestCase):
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

    def create_product(self):
        self.product = SmartWatch()
        self.product.product_name = "SmartWatch Model Y"
        self.product.price = 199.9
        self.product.discount = 20
        self.product.stock = 50
        self.product.description = (
            "SmartWatch Model Y: Advanced smartwatch with various features."
        )
        self.product.subheading = "SmartWatch Model Y: Advanced smartwatch"

        self.product.display_size = 1.4
        self.product.display_resolution = "360 x 360"

        self.product.operating_system = "Wear OS"
        self.product.memory = 4

        self.product.battery_capacity = 300

        self.product.weight = 0.5
        self.product.weight_units = "kg"

        self.product.color = "Black"

        self.product.bluetooth = True
        self.product.wifi = True
        self.product.nfc = False
        self.product.esim = True

        self.product.heart_rate_monitor = True
        self.product.step_counter = True
        self.product.sleep_tracker = True
        self.product.gps = True
        self.product.water_resistant = True
        self.product.music_player = True
        self.product.voice_assistant = True

        self.product.brand_id = 1
        self.product.category_id = 3
        self.product.product_image = "product_image_00003.jpg"

        db.session.add(self.product)
        db.session.commit()

    def test_product_have_product_name(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.product_name, "SmartWatch Model Y")

    def test_product_have_price(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.price, Decimal("199.90"))

    def test_product_have_discount(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.discount, 20)

    def test_product_have_stock(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.stock, 50)

    def test_product_have_sold(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.sold, 0)

    def test_product_have_display_size(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.display_size, 1.4)

    def test_product_have_display_resolution(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.display_resolution, "360 x 360")

    def test_product_have_operating_system(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.operating_system, "Wear OS")

    def test_product_have_memory(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.memory, 4)

    def test_product_have_battery_capacity(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.battery_capacity, 300)

    def test_product_have_weight(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertAlmostEqual(result.weight, 0.5, places=1)

    def test_product_have_weight_units(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.weight_units, "kg")

    def test_product_have_color(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertEqual(result.color, "Black")

    def test_product_have_bluetooth(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.bluetooth)

    def test_product_have_wifi(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.wifi)

    def test_product_have_nfc(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertFalse(result.nfc)

    def test_product_have_esim(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.esim)

    def test_product_have_heart_rate_monitor(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.heart_rate_monitor)

    def test_product_have_step_counter(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.step_counter)

    def test_product_have_sleep_tracker(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.sleep_tracker)

    def test_product_have_gps(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.gps)

    def test_product_have_water_resistant(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.water_resistant)

    def test_product_have_music_player(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.music_player)

    def test_product_have_voice_assistant(self):
        self.create_product()
        result = SmartWatch.query.filter_by(id=1).first()
        self.assertTrue(result.voice_assistant)