import pytest
import json
from unittest.mock import patch
from rest_framework.test import APIClient
from users.models import User
from storages.backends.s3boto3 import S3Boto3Storage
from google.oauth2 import id_token

@pytest.mark.django_db
class TestGoogleLoginAPI:
    def setup_method(self):
        """Set up test client and test data."""
        self.client = APIClient()
        self.valid_id_token = "valid_google_token"
        self.invalid_id_token = "invalid_google_token"
        self.mock_google_user_id = "123456789"
        self.mock_email = "testuser@example.com"
        self.mock_first_name = "Test"
        self.mock_last_name = "User"

    @patch("api.views.id_token.verify_oauth2_token")
    def test_google_login_existing_user(self, mock_verify_token):
        """Test logging in an existing user."""
        # Create an existing user
        User.objects.create(
            username=self.mock_email, 
            email=self.mock_email, 
            google_id=self.mock_google_user_id,
            first_name=self.mock_first_name, 
            last_name=self.mock_last_name
        )

        # Mock the token verification
        mock_verify_token.return_value = {
            'sub': self.mock_google_user_id,
            'email': self.mock_email,
            'name': f"{self.mock_first_name} {self.mock_last_name}",
            'given_name': self.mock_first_name,
            'family_name': self.mock_last_name
        }

        response = self.client.post("/api/auth/google/", {"id_token": self.valid_id_token}, format="json")
        assert response.status_code == 200
        assert "access" in response.data and "refresh" in response.data

    @patch("api.views.id_token.verify_oauth2_token")
    @patch("api.views.requests.get")
    def test_google_login_new_user(self, mock_get, mock_verify_token):
        """Test creating a new user with Google login."""
        # Mock the token verification
        mock_verify_token.return_value = {
            'sub': self.mock_google_user_id,
            'email': self.mock_email, 
            'first_name': self.mock_first_name, 
            'last_name': self.mock_last_name,
            'name': f"{self.mock_first_name} {self.mock_last_name}"
        }

        # Mock the avatar image download
        mock_get.return_value = type("Response", (object,), {
            "status_code": 200, 
            "content": b"fake_image_bytes"
        })

        response = self.client.post("/api/auth/google/", {"id_token": self.valid_id_token}, format="json")
        assert response.status_code == 200
        assert User.objects.filter(email=self.mock_email).exists()

    def test_google_login_missing_token(self):
        """Test missing Google id_token."""
        response = self.client.post("/api/auth/google/", {}, format="json")
        assert response.status_code == 400
        assert response.data["error"].startswith("Missing Google")

    @patch("api.views.id_token.verify_oauth2_token")
    def test_google_login_invalid_token(self, mock_verify_token):
        """Test invalid Google id_token."""
        mock_verify_token.side_effect = ValueError("Invalid token")

        response = self.client.post("/api/auth/google/", {"id_token": self.invalid_id_token}, format="json")
        assert response.status_code == 400
        assert response.data["error"].startswith("Invalid google")

    @patch("api.views.id_token.verify_oauth2_token")
    def test_google_login_updates_missing_username(self, mock_verify_token):
        """Test that Google login updates a user's missing username."""
        # Create a user with an empty username
        user = User.objects.create(
            username="",
            email=self.mock_email,
            google_id=self.mock_google_user_id,
            first_name=self.mock_first_name,
            last_name=self.mock_last_name
        )

        # Mock token verification
        mock_verify_token.return_value = {
            'sub': self.mock_google_user_id,
            'email': self.mock_email,
            'name': "John Doe"
        }

        # Call the Google login API
        response = self.client.post(
            "/api/auth/google/",
            {"id_token": self.valid_id_token},
            format="json"
        )

        # Refresh user from the database
        user.refresh_from_db()

        # Assertions
        assert response.status_code == 200
        assert user.username == "John Doe"  # Username should be updated from Google name
        assert "access" in response.data  # Ensure tokens are returned
        assert "refresh" in response.data

    @patch("api.views.id_token.verify_oauth2_token")
    @patch("api.views.requests.get")
    def test_google_login_profile_picture_download(self, mock_get, mock_verify_token):
        """Test downloading profile picture from Google login."""
        # Create a user with no profile image
        user = User.objects.create(
            username="testuser",
            email=self.mock_email,
            google_id=self.mock_google_user_id,
            first_name=self.mock_first_name,
            last_name=self.mock_last_name,
            profile_image=""  # No profile image yet
        )

        # Mock token verification
        mock_verify_token.return_value = {
            'sub': self.mock_google_user_id,
            'email': self.mock_email,
            'name': "John Doe",
            'picture': "https://example.com/profile.jpg"
        }

        # Mock image download response
        mock_get.return_value = type("Response", (object,), {
            "status_code": 200, 
            "content": b"fake_image_bytes"
        })

        # Call the Google login API
        response = self.client.post(
            "/api/auth/google/",
            {"id_token": self.valid_id_token},
            format="json"
        )

        # Refresh user from database
        user.refresh_from_db()

        # Assertions
        assert response.status_code == 200
        assert user.profile_image.name.endswith(".jpg")
        assert "access" in response.data
        assert "refresh" in response.data

    @patch("api.views.id_token.verify_oauth2_token")
    def test_google_login_missing_email(self, mock_verify_token):
        """Test handling of missing email in Google token."""
        # Mock token verification with no email
        mock_verify_token.return_value = {
            'sub': self.mock_google_user_id,
            'name': f"{self.mock_first_name} {self.mock_last_name}"
        }

        # Call the Google login API
        response = self.client.post(
            "/api/auth/google/",
            {"id_token": self.valid_id_token},
            format="json"
        )

        # Assertions
        assert response.status_code == 200
        # Verify that a random email was generated
        user = User.objects.get(google_id=self.mock_google_user_id)
        assert user.email
        assert "access" in response.data
        assert "refresh" in response.data

    @patch("api.views.id_token.verify_oauth2_token")
    def test_google_login_existing_user_by_email(self, mock_verify_token):
        """
        Test logging in a user that exists by email but doesn't have a google_id,
        covering lines 160 and 168-169.
        """
        # Create a user with the same email but no google_id
        User.objects.create(
            username="olduser", 
            email=self.mock_email, 
            google_id=None,
            first_name="Old", 
            last_name="User"
        )

        # Mock the token verification
        mock_verify_token.return_value = {
            'sub': self.mock_google_user_id,
            'email': self.mock_email,
            'name': f"{self.mock_first_name} {self.mock_last_name}",
            'given_name': self.mock_first_name,
            'family_name': self.mock_last_name
        }

        response = self.client.post("/api/auth/google/", {"id_token": self.valid_id_token}, format="json")
        
        # Refresh the user from the database
        user = User.objects.get(email=self.mock_email)
        
        assert response.status_code == 200
        assert user.google_id == self.mock_google_user_id
        assert "access" in response.data and "refresh" in response.data

    @patch("api.views.id_token.verify_oauth2_token")
    @patch("api.views.requests.get")
    def test_google_avatar_error_handling(self, mock_get, mock_verify_token):
        """
        Test error handling during avatar download.
        """
        # Create user with no profile image
        user = User.objects.create(
            username="avatar_error_user",
            google_id=self.mock_google_user_id
        )
        
        # Mock token verification with picture URL
        mock_verify_token.return_value = {
            "sub": self.mock_google_user_id,
            "email": self.mock_email,
            "picture": "https://example.com/avatar.jpg"
        }

        # Simulate a network error when fetching avatar
        mock_get.side_effect = Exception("Network error")

        # Call the endpoint - it should still succeed despite the avatar error
        response = self.client.post("/api/auth/google/", {"id_token": self.valid_id_token}, format="json")
        
        # Assert response was successful, login completed despite avatar error
        assert response.status_code == 200
        assert "access" in response.data
        assert "refresh" in response.data

    def teardown_method(self):
        """Ensure images are deleted from S3 after tests"""
        s3_storage = S3Boto3Storage()
        s3_storage.delete(f"profile_images/{self.mock_google_user_id}_avatar.jpg")