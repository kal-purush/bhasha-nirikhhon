from couchdb import Server
import delete_app
import unittest
import requests
import json

host = '127.0.0.1'
port = '5984'
database = 'blinkbox_files'
url = 'http://{0}:{1}/'.format(host, port)
server = Server(url=url)


class TestDeleteBulk(unittest.TestCase):

    def setUp(self):
        self.app = delete_app.app.test_client()

    def test_is_running(self):
        response = self.app.get('/')
        self.assertEqual(response.get_data(), 'Hello DOCKER & Delete')

    def test_delete_file(self):
        id1 = 'No_One_Ever_Will_Use_This_Id1'
        id2 = 'No_One_Ever_Will_Use_This_Id2'
        exp_date = 1000
        doc1 = {
            '_id': id1,
            'name': 'Mi Documento',
            'extension': '.algo',
            'size': 123,
            'upload_date': 1489947657,
            'expiring_date': exp_date,
            'owner_id': 'my_id',
            'shared': ['one@gmail.com', 'two@gmail.com'],
            }
        db = server[database]
        db.save(doc1)
        doc2 = {
            '_id': id2,
            'name': 'Mi Documento',
            'extension': '.algo',
            'size': 123,
            'upload_date': 1489947657,
            'expiring_date': exp_date,
            'owner_id': 'my_id',
            'shared': ['one@gmail.com', 'two@gmail.com'],
            }
        db.save(doc2)
        response = self.app.delete('/delete/bulk/' + str(exp_date))
        assert response.status_code == 202
        assert id1 not in db
        assert id2 not in db
        deleted = json.loads(response.get_data())
        assert id1 in deleted["deleted"]
        assert id2 in deleted["deleted"]


if __name__ == '__main__':
    unittest.main()


				