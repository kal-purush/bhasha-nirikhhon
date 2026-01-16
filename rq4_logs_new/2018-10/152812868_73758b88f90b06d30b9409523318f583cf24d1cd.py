import os

from rest_framework.test import APITestCase
from rest_framework import status


class TestStatusApi(APITestCase):
    URL = '/'
    CONTENT_TYPE = 'json'

    def setUp(self):
        os.environ['DOCKER_IMAGE_NAME'] = 'namespace/img'
        os.environ['DOCKER_IMAGE_TAG'] = '19.91'

    def test_api(self):
        response = self.client.get(self.URL, format=self.CONTENT_TYPE)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data['image'], os.environ['DOCKER_IMAGE_NAME'])
        self.assertEqual(response.data['tag'], os.environ['DOCKER_IMAGE_TAG'])
        self.assertIn('up_time', response.data)