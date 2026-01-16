import os
import pytest
import requests
from todo_app import app
from dotenv import load_dotenv, find_dotenv

@pytest.fixture
def client():
    file_path = find_dotenv('.env.test')
    load_dotenv(file_path, override=True)

    test_app = app.create_app()

    with test_app.test_client() as client:
        yield client

class StubResponse():
    def __init__(self, fake_response_data):
        self.fake_response_data = fake_response_data
    
    def json(self):
        return self.fake_response_data
    
def stub(url, params):
    test_board_id = os.environ.get('TRELLO_BOARD_ID')
    fake_response_data = None
    if url == f'https://api.trello.com/1/boards/{test_board_id}/lists':
        fake_response_data = [{
            'id': 'bearsboard27',
            'name': 'Not Started',
            'cards': [{'id': '43d8', 'idShort': '2', 'name': 'Task 2', 'desc': 'Task 2 not starte', 'due': '2020-06-10T15:48:26.091Z', 'dateLastActivity': '2020-06-10T15:48:26.091Z'}]
        }]
        return StubResponse(fake_response_data)
    raise Exception(f'Integration test stub no mock for url "{url}"')

def test_index_page(monkeypatch, client):
    monkeypatch.setattr(requests, 'get', stub)
    response = client.get('/')

    assert response.status_code == 200
    assert 'Task 2' in response.data.decode()