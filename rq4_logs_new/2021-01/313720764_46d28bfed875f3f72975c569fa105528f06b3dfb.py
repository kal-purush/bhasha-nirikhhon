from django.test import TestCase
from django.contrib.messages import get_messages
from django.shortcuts import reverse
from products.models import Product, Category, Colour

class TestBasketViews(TestCase):

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

    def test_view_basket(self):
        response = self.client.get(reverse('view_basket'), follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'basket/basket.html')

    """ establish how to pass quantity & redirect
    def test_add_to_basket(self):
        new_colour = Colour.objects.get(colour='test colour')
        response = self.client.post(reverse('add_to_basket', kwargs={'product_id': new_colour.product.id}, data={'quantity': '1', 'redirect_url': '/'}))
        messages = list(get_messages(response.wsgi_request))
        expected_message = f"Successfully added {quantity} x {new_colour.product.product_name} ({new_colour.colour}) to your basket"
    """


    def tearDown(self):
        new_category = Category.objects.get(name='test_category')
        new_product = Product.objects.get(product_name='test product')
        new_colour = Colour.objects.get(colour='test colour')
        
        del new_category
        del new_product
        del new_colour