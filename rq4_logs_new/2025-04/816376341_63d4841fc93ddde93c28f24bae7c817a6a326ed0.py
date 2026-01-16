from fastapi.testclient import TestClient

from .api import app

import json
import unittest


class TestAPI(unittest.TestCase):
    def test_api__healthcheck(authorizer):
        client = TestClient(app)
        response = client.get("/healthcheck")
        content = json.loads(response.content.decode("utf-8"))

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"
        assert content == {"healthcheck": True}