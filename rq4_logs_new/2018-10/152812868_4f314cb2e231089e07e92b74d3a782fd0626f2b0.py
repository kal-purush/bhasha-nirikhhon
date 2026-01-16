from django.contrib.auth import get_user_model
from django.test import TestCase

from rest_framework_jwt.settings import api_settings

from jwt_utils.handlers import jwt_payload_handler

User = get_user_model()


USER_CHI = {
    'username': 'Chi',
    'email': 'chi@foothub.com',
    'password': 'verystrong3'
}


class TestHandler(TestCase):

    def test_settings(self):
        self.assertEqual(api_settings.JWT_PAYLOAD_HANDLER, jwt_payload_handler)

    def test_payload_generation(self):
        user_chi = User.objects.create_user(**USER_CHI)

        payload = jwt_payload_handler(user_chi)

        self.assertEqual(len(payload), 4)
        self.assertIn('uuid', payload)
        self.assertIn('exp', payload)
        self.assertEqual(payload['username'], USER_CHI['username'])
        self.assertEqual(payload['email'], USER_CHI['email'])