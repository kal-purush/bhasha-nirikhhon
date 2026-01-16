from app.main import app
from common.helpers.auth import (
    create_access_token,
    decode_access_token,
    get_pwd_hash,
    verify_pwd,
)
from fastapi.testclient import TestClient

client = TestClient(app)


# FIX ME
def test_login_for_access_token():
    form_data = {"username": "test", "password": "test"}
    response = client.post("/auth/token", data=form_data)
    assert response.status_code == 200
    assert "token" in response.json()
    assert response.json()["token"]


# FIX ME
def test_login_for_access_token_bad_credentials():
    form_data = {"username": "test", "password": "bad"}
    response = client.post("/auth/token", data=form_data)
    assert response.status_code == 401
    assert "detail" in response.json()
    assert response.json()["detail"] == "Could not validate credentials"


def test_get_pwd_hash():
    pwd = "test"
    assert get_pwd_hash(pwd) != pwd


def test_verify_pwd():
    pwd = "test"
    pwd_hash = get_pwd_hash(pwd)
    assert verify_pwd(pwd, pwd_hash)
    assert not verify_pwd("bad", pwd_hash)


def test_create_access_token():
    data = {"sub": "test"}
    token = create_access_token(data)
    assert token


def test_decode_access_token():
    data = {"sub": "test"}
    token = create_access_token(data)
    assert decode_access_token(token)
    assert not decode_access_token("bad")