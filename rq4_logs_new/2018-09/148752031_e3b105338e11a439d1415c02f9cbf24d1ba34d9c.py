from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status

from content.models import EmailContent


class ShareLinkTest(TestCase):

    def test_share_link_ok(self):
        client = APIClient()

        # To send a share link, we need an email content
        EmailContent(
            type=EmailContent.SHARE_LINK,
            language_code='en',
            subject='Share link',
            text='{link}',
            html='<div>{link}</div>').save()

        response = client.post(
            path='/api/share-link/',
            data={
                'email': 'taufik@zimmermanzimmerman.nl',
                'language_code': 'en',
                'link': 'https://www.sample.com'
            },
            format='json',
            **{'REMOTE_HOST': 'localhost:3000'}
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_share_link_forbidden(self):
        """
        Test the status forbidden because the remote host
        is not registered on the settings.py
        """
        client = APIClient()

        # To send a share link, we need an email content
        EmailContent(
            type=EmailContent.SHARE_LINK,
            language_code='en',
            subject='Share link',
            text='{link}',
            html='<div>{link}</div>').save()

        response = client.post(
            path='/api/share-link/',
            data={
                'email': 'taufik@zimmermanzimmerman.nl',
                'language_code': 'en',
                'link': 'https://www.sample.com'
            },
            format='json'
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)