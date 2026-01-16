""" Libor Havránek App Copyright (C)  8.6 2023 """

import unittest

from myshop import create_app, db
from myshop.models.order_model import CustomerOrder
from myshop.tests.my_test_mixin import TestMixin


class TestCustomerOrderModel(TestMixin, unittest.TestCase):
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

    def create_customer_order(self):
        self.order = CustomerOrder()
        self.order.customer_id = 1
        self.order.customer_first_name = "John"
        self.order.customer_last_name = "Doe"
        self.order.customer_email = "john.doe@example.com"
        self.order.customer_phone_code = "+1"
        self.order.customer_phone = "1234567890"
        self.order.customer_city = "New York"
        self.order.customer_street = "Main Street"
        self.order.customer_zipcode = "12345"
        self.order.customer_info = "Additional info"

        self.order.payment_status = False
        self.order.total_price = 999.9

        self.order.products = (
            '[{"product_id": 1, "quantity": 2}, {"product_id": 2, "quantity": 1}]'
        )

        db.session.add(self.order)
        db.session.commit()

    def test_order_have_customer_id(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_id, 1)

    def test_order_have_customer_first_name(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_first_name, "John")

    def test_order_have_customer_last_name(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_last_name, "Doe")

    def test_order_have_customer_email(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_email, "john.doe@example.com")

    def test_order_have_customer_phone_code(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_phone_code, "+1")

    def test_order_have_customer_phone(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_phone, "1234567890")

    def test_order_have_customer_city(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_city, "New York")

    def test_order_have_customer_street(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_street, "Main Street")

    def test_order_have_customer_zipcode(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_zipcode, "12345")

    def test_order_have_customer_info(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.customer_info, "Additional info")

    def test_order_have_payment_status(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.payment_status, False)

    def test_order_have_total_price(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(result.total_price, 999.9)

    def test_order_have_order_date(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertIsNotNone(result.order_date)

    def test_order_have_products(self):
        self.create_customer_order()
        result = CustomerOrder.query.filter_by(id=1).first()
        self.assertEqual(
            result.products,
            '[{"product_id": 1, "quantity": 2}, {"product_id": 2, "quantity": 1}]',
        )