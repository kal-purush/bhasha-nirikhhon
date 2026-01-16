from django.test import TestCase
from django.contrib.auth.models import User
from django.utils import timezone
from django.urls import reverse


class AuthenticationTest(TestCase):

    def setUp(self):
        voter = User.objects.create_user("Yohn", "warrick@niflheim.realm", "monkey123")
        voter.first_name = "Yhjorn"
        voter.last_name = "Kraussmen"
        voter.save()

    def test_login(self):
        self.client.login(username="Yohn", password="monkey123")
        response = self.client.get(reverse("polls:index"))
        self.assertContains(response, "Yohn")
        self.assertContains(response, "Logout")

    def test_logout(self):
        response = self.client.get(reverse("polls:index"))
        self.assertContains(response, "Login")

