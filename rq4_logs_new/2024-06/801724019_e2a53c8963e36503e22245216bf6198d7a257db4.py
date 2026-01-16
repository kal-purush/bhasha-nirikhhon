from common.data_handlers.user_handler import UserHandler
from models.user import User

user_handler = UserHandler()


def test_create_user():
    user = User(username="test_user", password="test_password")
    result = user_handler.create_user(user)
    assert isinstance(result, User)
    assert result.username == user.username
    assert result.password == user.password


def test_get_user_by_id():
    user = User(username="test_user", password="test_password")
    result = user_handler.create_user(user)
    user_id = result.id
    result = user_handler.get_user_by_id(user_id)
    assert isinstance(result, User)
    assert result.id == user_id


def test_get_user_by_username():
    user = User(username="test_user", password="test_password")
    result = user_handler.create_user(user)
    username = result.username
    result = user_handler.get_user_by_username(username)
    assert isinstance(result, User)
    assert result.username == username


def test_update_user():
    pass