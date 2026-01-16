import pytest
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from src.customer.models import Customer, Location
from src.customer.serializers import CustomerSerializer


@pytest.fixture
def test_data():
    """Поднимает временные данные."""
    Location.objects.create(country='BMW', city='asd', street='RED', home=1000)
    location = Location.objects.all().first()
    Customer.objects.create(name='Test', location=location, balance=2000, query='gi', phone='+375333905555', age=15)


@pytest.mark.django_db
class TestCustomer(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_superuser(username='testuser', password='12345')

    def setUp(self):
        self.client.login(username='testuser', password='12345')

    @pytest.mark.usefixtures()
    def test_get_customer_list(self):
        """GET запрос к списку покупателей."""
        url = reverse('customers-list')
        response = self.client.get(url)
        assert response.status_code == status.HTTP_200_OK

    @pytest.mark.usefixtures('test_data')
    def test_post_customer_list(self):
        """POST запрос к списку покупателей."""
        url = reverse('customers-list')
        data = CustomerSerializer(Customer.objects.all().first()).data
        response = self.client.post(url, data=data, status='json')
        assert response.status_code == status.HTTP_201_CREATED
        assert response.data == data
