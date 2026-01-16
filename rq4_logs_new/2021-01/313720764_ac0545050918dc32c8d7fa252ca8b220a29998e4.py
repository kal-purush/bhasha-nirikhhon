from django.test import TestCase
from django.conf import settings
from checkout.models import Order, OrderLineItem
from products.models import Product, Category, Colour
from decimal import Decimal

class TestCheckoutModels(TestCase):

    def setUp(self):   
        new_category = Category.objects.create(
            name='test_category', 
            slug='test-category', 
            friendly_name='test category',
        )

        new_product = Product.objects.create(
            category=new_category,
            product_name='test product',
            price=1.00
            )

        new_colour = Colour.objects.create(
            product=new_product,
            colour='test colour',
            hex_value='000'
        )

        new_order = Order.objects.create(
            full_name='Mr test',
            email='test@test.com',
            phone_number='123245789',
            address_1='1 My Street',
            address_2='My Village',
            city='My City',
            county='My County',
            country='GB',
            postcode='AB12CD',
        )

        new_order_line_item = OrderLineItem.objects.create(
            order=new_order,
            product=new_product,
            colour=new_colour,
            quantity = 1)
    

    def test_order_str_method(self):
        new_order = Order.objects.get(full_name='Mr test')
        
        self.assertEqual(str(new_order), new_order.order_number)

    def test_order_update_order_totals_method(self):
        new_order = Order.objects.get(full_name='Mr test')
        new_product = Product.objects.get(product_name='test product')
        two_decimal_places = Decimal(10) ** -2 

        self.assertEqual(Decimal(new_order.order_total).quantize(two_decimal_places), new_product.price)
        self.assertEqual(Decimal(new_order.delivery_cost).quantize(two_decimal_places), Decimal(settings.STANDARD_DELIVERY_CHARGE).quantize(two_decimal_places))
        self.assertEqual(Decimal(new_order.grand_total).quantize(two_decimal_places), Decimal(new_order.order_total + new_order.delivery_cost).quantize(two_decimal_places))


    def test_order_free_delivery(self):
        new_order = Order.objects.get(full_name='Mr test')
        new_category = Category.objects.get(name='test_category')
        new_product = Product.objects.get(product_name='test product')
        new_colour = Colour.objects.get(colour='test colour')
        new_order_line_item_2 = OrderLineItem.objects.create(
            order=new_order,
            product=new_product,
            colour=new_colour,
            quantity = 50)
        two_decimal_places = Decimal(10) ** -2 

        self.assertEqual(Decimal(new_order.delivery_cost).quantize(two_decimal_places), 0)

    def test_order_line_item_str_method(self):
        new_order = Order.objects.get(full_name='Mr test')
        new_category = Category.objects.get(name='test_category')
        new_product = Product.objects.get(product_name='test product')
        new_colour = Colour.objects.get(colour='test colour')
        new_order_line_item_3 = OrderLineItem.objects.create(
            order=new_order,
            product=new_product,
            colour=new_colour,
            quantity = 2)

        self.assertEqual(str(new_order_line_item_3),
            f'Product: {new_product.product_name}, Colour: {new_colour.colour} added to order: {new_order.order_number}',
        )


    def tearDown(self):
        new_order = Order.objects.get(full_name='Mr test')
        new_category = Category.objects.get(name='test_category')
        new_product = Product.objects.get(product_name='test product')
        new_colour = Colour.objects.get(colour='test colour')
        new_order_line_item = OrderLineItem.objects.get(
            order=new_order,
            product=new_product,
            colour=new_colour,
            quantity = 1)

        del new_order
        del new_category
        del new_product
        del new_colour
        del new_order_line_item