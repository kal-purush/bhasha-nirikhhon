import json
from datetime import datetime, timedelta


from django.urls import reverse
from django.test import TestCase
from rest_framework.test import APITestCase, APIRequestFactory
from rest_framework import status
from user.tests.factories import UserFactory
from flight.tests.factories import FlightFactory
from user.models import UserProfile, User
from flight.models import Flight

from flight.messages.success import FLIGHT_CREATED
from flight.messages.error import INVALID_DATE


class TestFlightEndpoint(APITestCase):

    def _auth_header(self, token):
        return f'Bearer {token}'

    def _login(self, data):
        url = reverse('user:signin')
        return self.client.post(url, data, format='json')

    def setUp(self):
        password = 'password'
        self.admin = UserFactory(
            is_staff=True,
            email='admin@test.com',
            password=password
        )
        self.user = UserFactory(
            email='user@test.com',
            password=password
        )
        
        admin_token = self._login(dict(email=self.admin.email, password=password)).data['data']['token']
        
        self._admin_header = self._auth_header(admin_token)

        token = self._login(dict(email=self.user.email, password=password)).data['data']['token']

        self._header = self._auth_header(token)

        self.flight_data = {
            'departure_date': datetime.now() + timedelta(weeks=1),
            'return_date': datetime.now() + timedelta(weeks=3),
            'destination': 'MAD',
            'origin': 'NYC',
            'airline_code': 'B23C',
            'fare': 400000,
            'one_way': False,
            'stops': 2,
            'flight_class': 'economy',
            'status': 'scheduled',
            'travellers_capacity': 150
        }

        self.flight_data_two = {
            'departure_date': datetime.now() + timedelta(weeks=2),
            'return_date': datetime.now() + timedelta(weeks=4),
            'destination': 'BAC',
            'origin': 'SAN',
            'airline_code': 'B435',
            'fare': 350000,
            'one_way': True,
            'stops': 2,
            'flight_class': 'economy',
            'status': 'scheduled',
            'travellers_capacity': 100
        }

        self.invalid_flight_data = {
            'departure_date': datetime.now() + timedelta(weeks=2),
            'return_date': datetime.now() + timedelta(weeks=4),
            'destination': 'BAC',
            'origin': 'SAN',
            'airline_code': 'B435',
            'fare': 350000,
            'one_way': True,
            'stops': 2,
            'flight_class': 'wrong class',
            'status': 'wrong status',
            'travellers_capacity': 100
        }

        self.flight = FlightFactory(**self.flight_data_two)

    
    def test_create_flight_with_valid_data_succeeds(self):
        url = reverse('flight:flight-create-list')
        response = self.client.post(url, 
        self.flight_data, format='json', HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data['message'], FLIGHT_CREATED)
        assert response_data['data']['departure_date'].startswith(str(self.flight_data['departure_date'].date()))
        assert len(Flight.objects.all()) == 2

    def test_create_flight_with_invalid_data_fails(self):
        url = reverse('flight:flight-create-list')
        response = self.client.post(url, 
        self.invalid_flight_data, format='json', HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        assert response_data['errors']['status'][0].startswith('"wrong status" is not one of the permitted values')
        assert response_data['errors']['flight_class'][0].startswith('"wrong class" is not one of the permitted values')

    def test_get_all_flights_succeeds(self):
        url = reverse('flight:flight-create-list')

        response = self.client.get(url, HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        assert len(response_data['data']) == 1

    def test_get_a_flight_succeeds(self):
        url = reverse('flight:flight-retrieve-update', kwargs=dict(id=self.flight.id))

        response = self.client.get(url, HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data['data']['id'], self.flight.id)
        self.assertEqual(response_data['data']['airline_code'], self.flight.airline_code)
        self.assertEqual(response_data['data']['status'], self.flight.status)

    def test_get_a_flight_with_wrong_id_fails(self):
        url = reverse('flight:flight-retrieve-update', kwargs=dict(id=100))

        response = self.client.get(url, HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response_data['detail'], 'Not found.')
    
    def test_update_flight_with_valid_data_succeeds(self):
        url = reverse('flight:flight-retrieve-update', kwargs=dict(id=self.flight.id))

        flight_data = self.flight_data.copy()
        flight_data['status'] = 'delayed'

        response = self.client.patch(url, 
        flight_data, format='json', HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        assert response_data['data']['departure_date'].startswith(str(flight_data['departure_date'].date()))
        self.assertEqual(response_data['data']['status'], flight_data['status'])
        assert len(Flight.objects.all()) == 1

    def test_update_flight_with_wrong_id_fails(self):
        url = reverse('flight:flight-retrieve-update', kwargs=dict(id=100))

        response = self.client.patch(url, 
        self.flight_data, format='json', HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response_data['detail'], 'Not found.')
    
    def test_get_reservations_with_valid_id_succeeds(self):
        date = str(datetime.now().date())
        url = reverse(
            'flight:flight-reservations',
            kwargs=dict(id=self.flight.id)
        ) + '?date=' + date

        response = self.client.get(url, HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data['data']['reservations'], 0)

    def test_get_reservations_with_wrong_date_fails(self):
        date = str(datetime.now())
        url = reverse(
            'flight:flight-reservations',
            kwargs=dict(id=self.flight.id)
        ) + '?date=' + date

        response = self.client.get(url, HTTP_AUTHORIZATION=self._admin_header)
        response_data = response.data
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response_data['errors']['date'][0], INVALID_DATE.format(date))