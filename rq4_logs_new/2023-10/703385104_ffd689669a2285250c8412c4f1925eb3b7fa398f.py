import unittest
import json
from unittest.mock import MagicMock
from app.controllers.user_controller import login
from app.exceptions.custom_exceptions import CustomException
from app import app
import unittest
import requests


class TestLoginAPI(unittest.TestCase):
    base_url = "http://localhost:5000"  # Update with your actual URL

    def test_successful_login_with_email(self):
        url = f"{self.base_url}/login"
        payload = {
            "email": "shyam@gmail.com",  # Provide the email or phone number which exist in your database
            "phone": None,
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Login successful.", response.json()["message"])
        self.assertIn("user", response.json())

    def test_successful_login_with_phone(self):
        url = f"{self.base_url}/login"
        payload = {
            "email": None,
            "phone": "8690458967",  # Provide the phone number which exist in your database
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Login successful.", response.json()["message"])
        self.assertIn("user", response.json())

    def test_failed_login_invalid_request(self):
        url = f"{self.base_url}/login"
        payload = {}

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn(
            "Invalid request. Provide either email or phone.", response.json()["error"]
        )

    def test_failed_login_both_email_and_phone(self):
        url = f"{self.base_url}/login"
        payload = {
            "email": "",
            "phone": "",
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn(
            "Invalid request. Provide either email or phone.", response.json()["error"]
        )

    def test_failed_login_empty_email_and_phone(self):
        url = f"{self.base_url}/login"
        payload = {
            "email": "",  # Leave this fields empty
            "phone": "",  # Leave this fields empty
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn(
            "Invalid request. Provide either email or phone.", response.json()["error"]
        )

    def test_failed_login_user_not_found_with_email(self):
        url = f"{self.base_url}/login"
        payload = {
            "email": "nonexistent@example.com",  # Provide the non-existing email
            "phone": None,
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 404)
        self.assertIn("User not found.", response.json()["error"])

    def test_failed_login_user_not_found_with_phone(self):
        url = f"{self.base_url}/login"
        payload = {
            "email": None,
            "phone": "9999999999",  # Provide a non-existing phone number in your database
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 404)
        self.assertIn("User not found.", response.json()["error"])


if __name__ == "__main__":
    unittest.main()