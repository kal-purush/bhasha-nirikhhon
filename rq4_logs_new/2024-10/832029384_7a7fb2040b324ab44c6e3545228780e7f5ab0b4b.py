import pytest
import requests
from assertpy import assert_that
from requests import Response, Session
from requests.auth import HTTPBasicAuth

BASE_URL: str = "http://127.0.0.1:8080"


@pytest.fixture(scope="session", autouse=True)
def authorization() -> None:
    auth = HTTPBasicAuth('test_user', 'test_pass')

    response: Response = requests.post(f"{BASE_URL}/auth", auth=auth)

    assert_that(response.status_code).is_equal_to(200)
    access_token: str = response.json().get("access_token")

    current_session = Session()
    current_session.headers.update({'Authorization': 'Bearer ' + access_token})

    yield current_session