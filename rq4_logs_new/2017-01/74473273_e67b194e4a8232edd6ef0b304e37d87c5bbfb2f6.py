import json

from httmock import urlmatch, HTTMock

from mass_api_client.resources import DomainSample
from mass_api_client.resources import FileSample
from mass_api_client.resources import IPSample
from tests.httmock_test_case import HTTMockTestCase


class ObjectCreationTestCase(HTTMockTestCase):
    def assertCorrectHTTPDetailCreation(self, resource, path, data, data_path):
        with open(data_path) as data_file:
            response_data = json.load(data_file)

        @urlmatch(netloc=r'localhost', path=path)
        def mass_mock_creation(url, request):
            self.assertAuthorized(request)
            self.assertEqual(json.loads(request.body), data)
            return json.dumps(response_data)

        with HTTMock(mass_mock_creation):
            obj = resource.create(**data)
            self.assertEqual(response_data, obj._to_json())

    def assertCorrectHTTPDetailCreationWithFile(self, resource, path, metadata, data_path, filename, file):
        with open(data_path) as data_file:
            response_data = json.load(data_file)

        @urlmatch(netloc=r'localhost', path=path)
        def mass_mock_creation(url, request):
            self.assertAuthorized(request)
            self.assertHasFile(request, 'file', filename, file)
            self.assertHasFile(request, 'metadata', None, json.dumps(metadata), content_type='application/json')
            return json.dumps(response_data)

        with HTTMock(mass_mock_creation):
            obj = resource.create(filename=filename, file=file, **metadata)
            self.assertEqual(response_data, obj._to_json())

    def test_creating_domain_sample(self):
        data = {'domain': 'uni-bonn.de', 'tlp_level': 0}
        self.assertCorrectHTTPDetailCreation(DomainSample, r'/api/sample/submit_domain/', data,
                                             'tests/data/domain_sample.json')

    def test_creating_ip_sample(self):
        data = {'ip_address': '192.168.1.1', 'tlp_level': 0}
        self.assertCorrectHTTPDetailCreation(IPSample, r'/api/sample/submit_ip/', data,
                                             'tests/data/ip_sample.json')

    def test_creating_file_sample(self):
        with open('tests/data/test_data', 'rb') as file:
            data = {'tlp_level': 0}
            self.assertCorrectHTTPDetailCreationWithFile(FileSample, r'/api/sample/submit_file/', data,
                                                         'tests/data/ip_sample.json', 'test_data', file)