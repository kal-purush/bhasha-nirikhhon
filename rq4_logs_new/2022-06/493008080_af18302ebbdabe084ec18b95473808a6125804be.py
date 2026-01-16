import json
from src.app import app
import pytest
from src.repositories.session_repo import SessionRepository
from src.repositories.user_repo import UserRepository
    
username = 'user_test_login'
password = '123Fulano@'

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        client.post('/signup', data={
            'username': username,
            'password': password
        }, follow_redirects=True)
        yield client
        UserRepository().delete_user_by_username(username)

class TestLogin:
    def test_login_returns_session(self, client):
        response = client.post('/login', data={
            'username': username,
            'password': password
        }, follow_redirects=True)
        assert response.status_code == 200
        data = json.loads(response.get_data(as_text=True))
        session = SessionRepository().get_session_by_id(data['session_id'])
        assert session
        assert session.user.username == username
        SessionRepository().delete_session_by_id(session.id)
    
    def test_does_not_login_with_incorrect_password(self, client):
        response = client.post('/login', data={
            'username': username,
            'password': 'WrongPassword@123'
        }, follow_redirects=True)
        assert response.status_code == 401

    def test_does_not_login_with_nonexistent_username(self, client):
        response = client.post('/login', data={
            'username': 'nonexistent',
            'password': password
        }, follow_redirects=True)
        assert response.status_code == 401