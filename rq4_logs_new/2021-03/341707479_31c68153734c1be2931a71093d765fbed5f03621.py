# from django.test import TestCase
# from orders.models import Order
# from .models import Inventory


# class IventoryModelTest(TestCase):
#     @classmethod
#     def setUpTestData(cls):
#         # create product model
#         cls.order = Order.objects.create(
#             total_price=10,
#             description='test test',
#             status='TESTE',
#             client_id=1
#         )
#         # create invetory model
#         cls.inventory = Inventory.objects.create(
#             amount=15,
#             transaction_time=timezone.now(),
#             transaction_type='register',
#             product_id=cls.product
#         )

#     def test_create_inventory_model(self):
#         self.assertEqual(self.product.name, 'product test')
#         self.assertEqual(self.inventory.id, 1)
#         self.assertEqual(self.inventory.product_id.id, 1)
#         self.assertEqual(self.inventory.transaction_type, "register")