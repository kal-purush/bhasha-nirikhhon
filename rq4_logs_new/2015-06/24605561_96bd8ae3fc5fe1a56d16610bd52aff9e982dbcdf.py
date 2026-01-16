import unittest
from models import *
import mock


class TestElectionModel(unittest.TestCase):

    @mock.patch('election.google.appengine.ext.db')
    @mock.patch('election.google.appengine.ext.db.Model')
    def test_properties_exist_upon_creation(self, db_mock):
        Election()
        db_mock.ReferenceProperty.assert_called_with('any path')
        db_mock.StringProperty.assert_called_with('any path')
        db_mock.DateProperty.assert_called_with('any path')

    @mock.patch('election.google.appengine.ext.db')
    def test_to_json_returns_correct_info(self, db_mock):
        election = Election()
        election.user = 'foo'
        election.title = 'bar'
        election.date_added = 'today'
        election.date_expired = 'tomorrow'
        election.email = 'a@b.com'
        election.organization = 'the dark side'
        self.assertEqual({
            'user': election.user,
            'title': election.title,
            'date_added': election.date_added,
            'date_expired': election.date_expired,
            'email': election.email,
            'organization': election.organization
            }, election.to_json())