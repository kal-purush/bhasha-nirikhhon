import unittest
import json
from app import app
import requests
from unittest.mock import patch


class TestSignupAPI(unittest.TestCase):
    base_url = "http://localhost:5000"

    def test_successful_signup(self):
        url = f"{self.base_url}/signup"
        payload = {
            "full_name": "Johnthan",
            "email": "johnthan@gmail.com",
            "phone": "1234567895",
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 201)
        self.assertIn("User registered successfully.", response.json()["message"])

    def test_failed_signup_existing_email(self):
        url = "http://localhost:5000/signup"
        existing_user_data = {
            "full_name": "ram gopal",
            "email": "ram@gmail.com",
            "phone": None,
        }

        response = requests.post(url, json=existing_user_data)

        self.assertEqual(response.status_code, 400)
        expected_message = {
            "error": "('User already registered with this email or phone.', 400)"
        }
        self.assertEqual(response.json(), expected_message)

    def test_failed_signup_empty_full_name(self):
        url = f"{self.base_url}/signup"
        payload = {"full_name": "", "email": "johndoe@example.com", "phone": None}

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn(
            "Invalid request. Provide full name and either email or phone.",
            response.json()["error"],
        )

    def test_failed_signup_missing_email_and_phone(self):
        url = f"{self.base_url}/signup"
        payload = {"full_name": "John Doe", "email": None, "phone": None}

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn(
            "Invalid request. Provide full name and either email or phone.",
            response.json()["error"],
        )

    def test_failed_signup_invalid_email_format(self):
        url = f"{self.base_url}/signup"
        payload = {
            "full_name": "John Doe",
            "email": "invalidemail",
            "phone": None,
        }

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid email format.", response.json()["error"])

    def test_failed_signup_invalid_request(self):
        url = f"{self.base_url}/signup"
        payload = {}

        response = requests.post(url, json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn(
            "Invalid request. Provide full name and either email or phone.",
            response.json()["error"],
        )