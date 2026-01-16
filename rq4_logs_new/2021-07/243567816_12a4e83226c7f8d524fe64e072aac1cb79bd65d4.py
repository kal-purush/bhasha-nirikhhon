import unittest
from unittest import TestCase
from unittest.mock import patch
from broker_app import app as _app


class ExportToSpreadsheetTestCase(TestCase):

    def setUp(self):
        _app.testing = True

    @patch('broker_app.request')
    @patch('broker_app.ExportToSpreadsheetService.export')
    @patch('broker_app.IngestApi')
    def test_export_to_spreadsheet_route(self, mock_ingest, mock_export, mock_request):
        # given
        # noting here yet

        # when
        with _app.test_client() as app:
            response = app.get('/submissions/sub-001/spreadsheet')

            # then
            self.assertEqual(response.status_code, 200)
            mock_export.assert_called_with('sub-001')


if __name__ == '__main__':
    unittest.main()