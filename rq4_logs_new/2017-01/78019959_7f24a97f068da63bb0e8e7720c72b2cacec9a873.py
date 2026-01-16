# coding: utf-8
from django.tests import TestCase
from django.urls import reverse


class HomeSiteViewTestCase(TestCase):
    def test_correct_response(self):
        response = self.client.get(reverse('home'))

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'core/index.html')
        
        