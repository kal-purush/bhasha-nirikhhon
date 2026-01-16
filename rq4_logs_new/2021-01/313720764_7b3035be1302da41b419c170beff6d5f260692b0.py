from django.test import TestCase
from checkout.forms import OrderForm

class TestCheckoutForms(TestCase):
    def test_checkout_form_explicit_fields(self):
        form = OrderForm()
        self.assertEqual(
            form.Meta.fields,
            (
                'full_name',
                'email',
                'phone_number',
                'address_1',
                'address_2',
                'city',
                'county',
                'country',
                'postcode', 
            ),
        )

    def test_checkout_form_required_fields(self):
        form = OrderForm(
            {
                'full_name': '',
                'email': '',
                'phone_number': '',
                'address_1': '',
                'city': '',
                'country': '',
            }
        )

        self.assertFalse(form.is_valid())
        self.assertEqual(form.errors['full_name'][0], 'This field is required.')
        self.assertEqual(form.errors['email'][0], 'This field is required.')
        self.assertEqual(form.errors['phone_number'][0], 'This field is required.')
        self.assertEqual(form.errors['city'][0], 'This field is required.')
        self.assertEqual(form.errors['country'][0], 'This field is required.')