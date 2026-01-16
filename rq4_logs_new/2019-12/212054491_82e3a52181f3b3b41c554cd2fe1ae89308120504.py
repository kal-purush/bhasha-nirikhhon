import pytest, json
from main import logger
from unittest import mock


@pytest.mark.parametrize(
    ('headers', 'logger_message', 'http_code'),
    (
        ('', 'In post request Authorization header missing', 401),
        ({'Authorization': 'Bearer '}, 'In Authorization header token missing', 401),
        ({'Authorization': 'Bearer eyJhbGciOiJIJ9.eyJzdWIiOifQ.SflKxwQssw5c'}, 'Got invalid token', 401)
    ),
    ids = ['No header case', 'Empty header Authorization', 'Invalid token in header']
)
def test_sign_in_error_cases(client, app, headers, logger_message, http_code):
    with mock.patch.object(logger, 'info') as mock_info:
        response_error = client.post('/my-todos', headers = headers)
        assert logger_message in mock_info.call_args[0][0]
        assert http_code == response_error.status_code

def test_sign_in_valid_token(client, app):
    response_login = client.post('/login', data = json.dumps({"name": "denchik", "password": "foobar"}), headers = {'content-type': 'application/json'})
    recieved_token = json.loads(response_login.data)
    response_sign_in = client.post('/my-todos', headers = {'Authorization': recieved_token['token']})    
    assert 200 == response_sign_in.status_code
    assert 'toDoLists' in str(response_sign_in.data)


    
    
    