from django.contrib.auth.hashers import check_password
from django.contrib.auth.models import User
from django.urls import reverse
from django.test import Client, TransactionTestCase


class TestSignup(TransactionTestCase):

    def test_stage_ranking(self):
        target = reverse('signup')
        c = Client()
        response = c.get(target)
        self.assertTemplateUsed(response, "registration/register.html")
        response = c.post(target, {"username": "testusername", "password1": "GreatWord", "password2": "GreatWord", "privacy_consent": True})
        self.assertEqual(response.status_code, 302)
        user = User.objects.filter(username="testusername").first()
        self.assertTrue(check_password("GreatWord", user.password))