from openprocurement.integrations.edr.tests.base import BaseWebTest

class TestHealth(BaseWebTest):
    """ Test health view"""

    def test_get(self):
        response = self.app.get('/health')
        self.assertEqual(response.status, "200 OK")
