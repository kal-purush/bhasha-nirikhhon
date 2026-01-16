from django.test import TestCase
from profiles.forms import UserProfileForm


class TestUserProfileForms(TestCase):
    def test_profile_form_user_field_excluded(self):
        form = UserProfileForm()
        self.assertEqual(form.Meta.exclude, ('user',))

    def test_profile_form_is_valid(self):
        form = UserProfileForm(
            {
                'default_phone_number': '123245789',
                'default_street_address_1': '1 My Street',
                'default_street_address_2': 'My Village',
                'default_city': 'My City',
                'default_county': 'My County',
                'default_country': 'GB',
                'default_postcode': 'AB12CD',
            }
        )
        self.assertTrue(form.is_valid())