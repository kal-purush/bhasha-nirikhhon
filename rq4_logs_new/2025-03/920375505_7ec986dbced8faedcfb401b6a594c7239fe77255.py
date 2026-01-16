import pytest
import json
from unittest.mock import patch, MagicMock
from django.urls import reverse
from django.test import Client, TestCase
from rest_framework.test import APIClient
from users.models import User


class AuthIntegrationTest(TestCase):
    """
    End-to-end tests for authentication flow, testing the entire
    auth flow from API call to database changes.
    """
    
    def setUp(self):
        """Set up for integration tests."""
        self.client = Client()
        self.api_client = APIClient()
        # Mock Facebook token and response data
        self.mock_fb_token = "mock_valid_facebook_token"
        self.mock_fb_user_id = "123456789"
        self.mock_email = "test.user@example.com"
        self.mock_name = "Test User"
        self.mock_first_name = "Test"
        self.mock_last_name = "User"
        
    @patch("api.views.requests.get")
    def test_complete_auth_flow_new_user(self, mock_requests):
        """
        Test the complete authentication flow for a new user:
        1. Call Facebook login API
        2. Verify database user creation
        3. Verify JWT token response
        4. Test accessing a protected endpoint
        """
        # Mock the Facebook API responses
        # First response is for token verification
        # Second response is for user data
        mock_responses = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_fb_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_fb_user_id,
                    "email": self.mock_email,
                    "name": self.mock_name,
                    "first_name": self.mock_first_name,
                    "last_name": self.mock_last_name,
                    "picture": {"data": {"url": "https://example.com/avatar.jpg"}}
                }
            )
        ]
        mock_requests.side_effect = mock_responses
        
        # 1. Call Facebook login API
        initial_user_count = User.objects.count()
        response = self.api_client.post(
            "/api/auth/facebook/", 
            {"access_token": self.mock_fb_token}, 
            format="json"
        )
        
        # Verify successful response
        self.assertEqual(response.status_code, 200)
        self.assertIn("access", response.data)
        self.assertIn("refresh", response.data)
        
        # Store tokens for later use
        access_token = response.data["access"]
        refresh_token = response.data["refresh"]
        
        # 2. Verify database user creation
        self.assertEqual(User.objects.count(), initial_user_count + 1)
        user = User.objects.get(facebook_id=self.mock_fb_user_id)
        self.assertEqual(user.email, self.mock_email)
        self.assertEqual(user.first_name, self.mock_first_name)
        self.assertEqual(user.last_name, self.mock_last_name)
        
        # 3. Verify token usage on protected endpoint
        # Set authorization header with the received token
        self.api_client.credentials(HTTP_AUTHORIZATION=f"Bearer {access_token}")
        
        # Call a protected endpoint (example: get user profile)
        # We need to use participants to usernames API since it's a protected endpoint
        response = self.api_client.get(
            "/api/users/idtoname/",
            {"participants": str(user.id)}
        )
        
        # Verify we can access protected resources
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["usernames"], [user.username])
        
    @patch("api.views.requests.get")
    def test_complete_auth_flow_existing_user(self, mock_requests):
        """
        Test the complete authentication flow for an existing user:
        1. Create user in database
        2. Call Facebook login API
        3. Verify user is not duplicated
        4. Verify JWT token response
        """
        # 1. Create existing user
        existing_user = User.objects.create(
            username=self.mock_email,
            email=self.mock_email,
            facebook_id=self.mock_fb_user_id,
            first_name=self.mock_first_name,
            last_name=self.mock_last_name
        )
        
        # Mock the Facebook API responses
        mock_responses = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_fb_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_fb_user_id,
                    "email": self.mock_email,
                    "name": self.mock_name,
                    "first_name": self.mock_first_name,
                    "last_name": self.mock_last_name,
                    "picture": {"data": {"url": "https://example.com/avatar.jpg"}}
                }
            )
        ]
        mock_requests.side_effect = mock_responses
        
        # 2. Call Facebook login API
        initial_user_count = User.objects.count()
        response = self.api_client.post(
            "/api/auth/facebook/", 
            {"access_token": self.mock_fb_token}, 
            format="json"
        )
        
        # 3. Verify no new user was created (same count)
        self.assertEqual(User.objects.count(), initial_user_count)
        
        # 4. Verify successful token response
        self.assertEqual(response.status_code, 200)
        self.assertIn("access", response.data)
        self.assertIn("refresh", response.data)

    @patch("api.views.requests.get")
    def test_facebook_token_validation_failure(self, mock_requests):
        """Test handling of invalid Facebook token."""
        # Mock invalid token response
        mock_requests.return_value = MagicMock(
            status_code=400,
            json=lambda: {"error": {"message": "Invalid OAuth access token."}}
        )
        
        # Call API with invalid token
        initial_user_count = User.objects.count()
        response = self.api_client.post(
            "/api/auth/facebook/", 
            {"access_token": "invalid_token"}, 
            format="json"
        )
        
        # Verify error response
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["error"], "Cannot verify Facebook token")
        
        # Verify no user was created
        self.assertEqual(User.objects.count(), initial_user_count)

    def test_protected_endpoint_without_auth(self):
        """Test that protected endpoints deny access without authentication."""
        # Create a test user
        test_user = User.objects.create(
            username="protected_test",
            email="protected@example.com"
        )
        
        # Attempt to access protected endpoint without auth
        response = self.api_client.get(
            "/api/users/idtoname/",
            {"participants": str(test_user.id)}
        )
        
        # Verify unauthorized response (401)
        self.assertEqual(response.status_code, 401)