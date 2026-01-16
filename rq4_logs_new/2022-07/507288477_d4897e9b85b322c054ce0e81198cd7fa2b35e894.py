import pytest
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from django.test.client import Client
from rest_framework.test import APITestCase
import random

from src.dealership.models import Location
from src.dealership.serializers import LocationSerializer


@pytest.fixture
def test_data():
    """Поднимает временные данные."""
    Location.objects.create(country='BMW', city='asd', street='RED', home=1000)


@pytest.mark.django_db
class TestCars(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_superuser(username='testuser', password='12345')

    def setUp(self):
        self.client.login(username='testuser', password='12345')

    # @pytest.mark.usefixtures()
    def test_get_cars_list(self):
        """GET запрос к списку машин."""
        url = reverse('locations-list')
        response = self.client.get(url)
        assert response.status_code == status.HTTP_200_OK

    @pytest.mark.usefixtures('test_data')
    def test_post_cars_list(self):
        """POST запрос к списку машин."""
        url = reverse('locations-list')
        data = LocationSerializer(Location.objects.all().first()).data
        print(data)
        response = self.client.post(url, data=data, status='json')
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data == data
