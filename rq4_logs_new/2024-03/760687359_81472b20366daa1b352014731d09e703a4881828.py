from django.test import TestCase
from django.db.utils import IntegrityError
from .models import Gym, Owner
from random import randint

class GymModelTest(TestCase):
    fixtures = ['owner/fixtures/owner.json', 'user/fixtures/user.json']

    @classmethod
    def setUpTestData(cls):
        owner = Owner.objects.get(name='Mohammed')
        Gym.objects.create(
            name='Test Gym',
            address='123 Test St',
            phone_number=123456789,
            descripcion='Test Description',
            zip_code=12345,
            email='test@example.com',
            owner=owner
        )

    def test_gym_id_is_random(self):
        gym = Gym.objects.get(name='Test Gym')
        another_gym = Gym.objects.create(
            name='Another Gym',
            address='456 Another St',
            phone_number=987654321,
            descripcion='Another Description',
            zip_code=54321,
            email='another@example.com',
            owner=Owner.objects.get(name='Mohammed')
        )
        self.assertNotEqual(gym.id, another_gym.id)

    def test_gym_name_max_length(self):
        gym = Gym.objects.get(name='Test Gym')
        max_length = gym._meta.get_field('name').max_length
        self.assertEquals(max_length, 50)

    def test_gym_email(self):
        gym = Gym.objects.get(name='Test Gym')
        self.assertTrue('@' in gym.email)

    def test_gym_owner_relationship(self):
        gym = Gym.objects.get(name='Test Gym')
        owner = Owner.objects.get(name='Mohammed')
        self.assertEqual(gym.owner, owner)

    def test_gym_without_owner(self):
        with self.assertRaises(IntegrityError):
            Gym.objects.create(
                name='Gym Without Owner',
                address='123 Test St',
                phone_number=123456789,
                descripcion='Test Description',
                zip_code=12345,
                email='test@example.com'
            )
    
    def test_invalid_phone_number(self):
        with self.assertRaises(ValueError):
            Gym.objects.create(
                name='Test Gym',
                address='123 Test St',
                phone_number='invalid_phone_number',  # Número de teléfono inválido
                descripcion='Test Description',
                zip_code=12345,
                email='test@example.com'
            )

    def test_invalid_zip_code(self):
        with self.assertRaises(ValueError):
            Gym.objects.create(
                name='Test Gym',
                address='123 Test St',
                phone_number=1234567890,
                descripcion='Test Description',
                zip_code='invalid_zip_code',  # Zip code inválido
                email='test@example.com'
            )