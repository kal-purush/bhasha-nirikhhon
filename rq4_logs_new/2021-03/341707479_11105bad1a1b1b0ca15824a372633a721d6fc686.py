from django.test import TestCase
from django.utils import timezone

from products.models import Product
from ..models import Inventory


class IventoryModelTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        # create product model
        cls.product = Product.objects.create(
            name='product test',
            price=4.5,
            description='description test for product'
        )
        # create invetory model
        cls.inventory = Inventory.objects.create(
            amount=15,
            transaction_time=timezone.now(),
            transaction_type='register',
            product_id=cls.product
        )

    def test_create_inventory_model(self):
        self.assertEqual(self.product.name, 'product test')
        self.assertEqual(self.inventory.id, 1)
        self.assertEqual(self.inventory.product_id.id, 1)
        self.assertEqual(self.inventory.transaction_type, "register")