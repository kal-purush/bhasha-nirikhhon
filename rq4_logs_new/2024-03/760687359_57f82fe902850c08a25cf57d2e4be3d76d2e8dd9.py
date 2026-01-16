from django.test import TestCase
from rest_framework.test import APIRequestFactory
from .views import UserListView, UserCreateView, UserUpdateView, UserDeleteView
from .models import CustomUser
from .serializers import CustomUserSerializer

class UserViewsTestCase(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.user = CustomUser.objects.create(username='testuser', email='test@example.com')

    def test_user_list_view(self):
        request = self.factory.get('/users/')
        response = UserListView.as_view()(request)
        self.assertEqual(response.status_code, 200)

    def test_user_create_view(self):
        data = {'username': 'newuser', 'email': 'newuser@example.com'}
        request = self.factory.post('/users/', data)
        response = UserCreateView.as_view()(request)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(CustomUser.objects.filter(username='newuser').exists())

    def test_user_update_view(self):
        data = {'username': 'updateduser', 'email': 'updateduser@example.com'}
        request = self.factory.post(f'/users/{self.user.pk}/', data)
        response = UserUpdateView.as_view()(request, pk=self.user.pk)
        self.assertEqual(response.status_code, 200)
        self.user.refresh_from_db()
        self.assertEqual(self.user.username, 'updateduser')

    def test_user_delete_view(self):
        request = self.factory.delete(f'/users/{self.user.pk}/')
        response = UserDeleteView.as_view()(request, pk=self.user.pk)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(CustomUser.objects.filter(pk=self.user.pk).exists())