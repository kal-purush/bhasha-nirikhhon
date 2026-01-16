import json
from pprint import pformat

from rest_framework.test import APITestCase


class APITestCaseMixin:
    """
    Assert a given HTTP status code, if not print body content
    """
    def assertStatusCode(self, response, desired_status_code):
        try:
            msg = pformat(json.loads(response.content))
        except:
            msg = ''

        self.assertEqual(
            response.status_code,
            desired_status_code,
            msg="Status code was {0}, response was \n{1}".format(
                response.status_code,
                msg))


class NiceAPITestCase(APITestCaseMixin, APITestCase):
    pass