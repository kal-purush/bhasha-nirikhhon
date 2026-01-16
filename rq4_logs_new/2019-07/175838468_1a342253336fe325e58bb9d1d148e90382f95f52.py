from unittest.mock import patch

import pytest
from flask import url_for

from tomaco.conftest import USER_EMAIL
from tomaco.decorators import login_required, session


@pytest.mark.usefixtures("app")
class TestLoginRequired:
    @patch.object(session, "get", return_value=USER_EMAIL)
    def test_should_execute_the_decorated_function_when_user_is_authenticated(
        self, _sessionMock
    ):
        expected = "should-be-decorated-function-return"

        @login_required
        def fake_function():
            return expected

        assert fake_function() is expected

    @patch.object(session, "get", return_value=None)
    def test_should_redirect_to_login_when_user_is_not_authenticated(
        self, _sessionMock
    ):
        @login_required
        def fake_function():
            return "should-not-be-executed"

        result = fake_function()

        assert result.status_code == 302
        assert result.location == url_for("login")