from unittest.mock import patch

import pytest

from src.dto import auth_dto
from tests.base_test import BaseTest
from tests.web.web_application_test import WebApplicationTest


class TestAuthRouter(BaseTest, WebApplicationTest):

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.get")
    @patch("aiohttp.ClientSession.post")
    async def test_get_canvas_auth_data(
        self,
        mock_post,
        mock_get,
        client,
        canvas_ok_response,
        create_canvas_user,
        cleanup_all,
    ):
        mock_get.return_value = canvas_ok_response
        mock_post.return_value = canvas_ok_response
        create_canvas_user(username="test@gmail.com", password="test-pwd")
        dto = auth_dto.LoginRequest(username="test@gmail.com", password="test-pwd")
        response = client.post("/api/auth/v1", json=dto.dict())
        assert response.status_code == 200
        data = response.json()
        assert data == {
            "_csrf_token": "6D2%2FyVAKZOxBRl6qZW",
            "_legacy_normandy_session": "pG-loNiNeyypme8vr5TO",
            "_normandy_session": "pG-loNiNeyypme8vr5TO",
            "log_session_id": "9f8187d804aaf1d15913",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "login_request",
        [
            auth_dto.LoginRequest(username="incorrect@gmail.com", password="test-pwd"),
            auth_dto.LoginRequest(username="test@gmail.com", password="incorrect-pwd"),
        ],
    )
    async def test_get_canvas_auth_data_should_raise_on_invalid_credentials(
        self,
        client,
        login_request,
        create_canvas_user,
        cleanup_all,
    ):
        create_canvas_user(username="test@gmail.com", password="test-pwd")
        response = client.post("/api/auth/v1", json=login_request.dict())
        assert response.status_code == 401
        data = response.json()
        assert data["message"] == "_error_msg_invalid_credentials"

    @pytest.mark.asyncio
    async def test_get_canvas_auth_data_should_raise_when_canvas_user_not_found(
        self,
        client,
        create_user,
        cleanup_all,
    ):
        dto = auth_dto.LoginRequest(username="test@gmail.com", password="test-pwd")
        create_user(username="test@gmail.com", password="test-pwd")
        response = client.post("/api/auth/v1", json=dto.model_dump())
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == "_error_msg_canvas_user_not_found:test@gmail.com"