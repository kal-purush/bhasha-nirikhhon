import pytest
from app import create_app

@pytest.fixture
def client():
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_index_route(client):
    response = client.get('/')
    assert response.status_code == 200
    assert b'Message Board' in response.data

def test_add_message(client):
    response = client.post('/add_message', data={'message': 'Test message'})
    assert response.status_code == 302  # Redirect
    
    # Check if message appears on index page
    response = client.get('/')
    assert b'Test message' in response.data