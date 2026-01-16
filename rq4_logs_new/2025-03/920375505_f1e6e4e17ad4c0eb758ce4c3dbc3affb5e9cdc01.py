import pytest
import os
import io
from unittest.mock import patch, MagicMock, Mock
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import override_settings
from rest_framework.test import APIClient
from rest_framework import status
from users.models import User
from django.conf import settings
from PIL import Image
from io import BytesIO
from storages.backends.s3boto3 import S3Boto3Storage

@pytest.mark.django_db
class TestFacebookLoginCoverage:
    def setup_method(self):
        """Set up test client and test data."""
        self.client = APIClient()
        self.valid_access_token = "valid_facebook_token"
        self.mock_facebook_user_id = "123456789"
        self.mock_email = "testuser@example.com"
        self.mock_first_name = "Test"
        self.mock_last_name = "User"
        self.mock_name = "Test User"

    @patch("api.views.requests.get")
    def test_facebook_token_validation_error(self, mock_get):
        """Test handling of failed token validation (line 37)."""
        # Mock the token validation response as failed
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"is_valid": False, "user_id": self.mock_facebook_user_id}}
        )

        response = self.client.post("/api/auth/facebook/", {"access_token": self.valid_access_token}, format="json")
        assert response.status_code == 400
        assert response.data["error"] == "Invalid Facebook token"

    @patch("api.views.requests.get")
    def test_facebook_login_missing_name_field(self, mock_get):
        """
        Test Facebook login when the name field is missing
        (specifically tests line 63).
        """
        # Mock responses for token validation and user info without name field
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_facebook_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_facebook_user_id,
                    "email": self.mock_email,
                    "first_name": self.mock_first_name,
                    "last_name": self.mock_last_name,
                    # No "name" field here
                    "picture": {"data": {"url": "https://example.com/avatar.jpg"}}
                }
            )
        ]

        # Call the endpoint
        response = self.client.post("/api/auth/facebook/", {"access_token": self.valid_access_token}, format="json")
        
        # Check for successful creation of user with facebook ID as username prefix
        assert response.status_code == 200
        user = User.objects.get(facebook_id=self.mock_facebook_user_id)
        assert user.username.startswith(f"fb_{self.mock_facebook_user_id}")

    @patch("api.views.requests.get")
    def test_facebook_login_updating_existing_user(self, mock_get):
        """
        Test updating an existing user's fields that are empty but received from Facebook.
        """
        # Create user with empty fields
        user = User.objects.create(
            username="test_username",
            email="",  # Empty email
            facebook_id=self.mock_facebook_user_id,
            first_name="",  # Empty first name
            last_name=""  # Empty last name
        )

        # Mock responses for token validation and user info
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_facebook_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_facebook_user_id,
                    "email": self.mock_email,
                    "name": self.mock_name,
                    "first_name": self.mock_first_name,
                    "last_name": self.mock_last_name,
                    "picture": {"data": {"url": "https://example.com/avatar.jpg"}}
                }
            )
        ]

        # Call the endpoint
        response = self.client.post("/api/auth/facebook/", {"access_token": self.valid_access_token}, format="json")
        
        # Refresh user from DB
        user.refresh_from_db()
        
        # Assert fields were updated
        assert response.status_code == 200
        assert user.email == self.mock_email
        assert user.first_name == self.mock_first_name
        assert user.last_name == self.mock_last_name

    @patch("api.views.requests.get")
    def test_facebook_login_find_by_email(self, mock_get):
        """
        Test finding an existing user by email and updating facebook_id.
        """
        # Create user with email but no facebook_id
        user = User.objects.create(
            username="email_user",
            email=self.mock_email,
            facebook_id=None  # No facebook_id
        )

        # Mock responses
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_facebook_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_facebook_user_id,
                    "email": self.mock_email,
                    "name": self.mock_name,
                    "first_name": self.mock_first_name,
                    "last_name": self.mock_last_name
                }
            )
        ]

        # Call the endpoint
        response = self.client.post("/api/auth/facebook/", {"access_token": self.valid_access_token}, format="json")
        
        # Refresh user from DB
        user.refresh_from_db()
        
        # Assert facebook_id was updated
        assert response.status_code == 200
        assert user.facebook_id == self.mock_facebook_user_id

    @patch("api.views.requests.get")
    @patch("api.views.ContentFile")
    def test_facebook_avatar_download(self, mock_content_file, mock_get):
        """
        Test downloading avatar from Facebook and saving it to the user profile.
        """
        # Create user with no profile image
        user = User.objects.create(
            username="avatar_user",
            facebook_id=self.mock_facebook_user_id
        )
        
        # Mock avatar content
        mock_avatar_content = b"fake_image_content"
        mock_content_file.return_value = mock_avatar_content
        
        # Mock responses
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_facebook_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_facebook_user_id,
                    "email": self.mock_email,
                    "picture": {"data": {"url": "https://example.com/avatar.jpg"}}
                }
            ),
            MagicMock(
                status_code=200,
                content=mock_avatar_content
            )
        ]

        # Call the endpoint
        response = self.client.post("/api/auth/facebook/", {"access_token": self.valid_access_token}, format="json")
        
        # Refresh user from DB
        user.refresh_from_db()
        
        # Assert response and profile image was updated
        assert response.status_code == 200
        assert "profile_image_url" in response.data
        assert user.profile_image.name is not None

    @patch("api.views.requests.get")
    def test_facebook_avatar_error_handling(self, mock_get):
        """
        Test error handling during avatar download (line 100).
        """
        # Create user with no profile image
        user = User.objects.create(
            username="avatar_error_user",
            facebook_id=self.mock_facebook_user_id
        )
        
        # Mock responses - the avatar request will raise an exception
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"is_valid": True, "user_id": self.mock_facebook_user_id}}
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "id": self.mock_facebook_user_id,
                    "email": self.mock_email,
                    "picture": {"data": {"url": "https://example.com/avatar.jpg"}}
                }
            ),
            Exception("Network error")  # Simulating a network error when fetching avatar
        ]

        # Call the endpoint - it should still succeed despite the avatar error
        response = self.client.post("/api/auth/facebook/", {"access_token": self.valid_access_token}, format="json")
        
        # Assert response was successful, login completed despite avatar error
        assert response.status_code == 200
        assert "access" in response.data
        assert "refresh" in response.data

    @patch("api.views.requests.get")
    def test_participants_to_usernames_empty_list(self, mock_get):
        """
        Test participants_to_usernames with an empty list after parsing.
        """
        # Create a user for authentication
        user = User.objects.create(username="test_user")
        client = APIClient()
        client.force_authenticate(user=user)
        
        # Send a parameter that will result in empty list after parsing
        # For example, a string with only commas
        response = client.get("/api/users/idtoname/", {"participants": ","})
        
        assert response.status_code == 400
        assert response.data["error"] == "'participants' should not be empty"

@pytest.mark.django_db
class TestProfileImageUpload:
    def setup_method(self):
        """Set up test client and user."""
        self.client = APIClient()
        self.user = User.objects.create(
            username="profile_image_user",
            email="profile@example.com"
        )
        self.client.force_authenticate(user=self.user)

    def create_test_image(self):
        """Create a simple in-memory test image."""
        
        image_io = BytesIO()
        image = Image.new("RGB", (100, 100), color="red")
        image.save(image_io, format="JPEG")
        image_io.seek(0)

        return image_io

    @patch('storages.backends.s3boto3.S3Boto3Storage.delete')
    def test_upload_profile_image_new(self, mock_s3_delete):
        """
        Test uploading a profile image when the user doesn't have one.
        """
        image_file = self.create_test_image()
        upload_file = SimpleUploadedFile("test.jpg", image_file.read(), content_type="image/jpeg")

        response = self.client.post(
            "/api/users/upload-profile-image/",
            {"profile_image": upload_file},
            format="multipart"
        )

        self.user.refresh_from_db()

        assert response.status_code == status.HTTP_200_OK
        mock_s3_delete.assert_not_called()  # No old image to delete
        assert self.user.profile_image is not None

    @patch('storages.backends.s3boto3.S3Boto3Storage.delete')
    def test_upload_profile_image_replacing_existing(self, mock_s3_delete):
        """
        Test replacing an existing profile image.
        """
        # First, assign an existing image
        image_file = self.create_test_image()
        self.user.profile_image.save(
            "old_image.jpg",
            SimpleUploadedFile("old_image.jpg", image_file.read(), content_type="image/jpeg")
        )
        self.user.save()

        old_image_name = self.user.profile_image.name  # Store old name

        # Upload a new image
        new_image_file = self.create_test_image()
        upload_file = SimpleUploadedFile(
            "new_image.jpg", new_image_file.read(), content_type="image/jpeg"
        )

        response = self.client.post(
            "/api/users/upload-profile-image/",
            {"profile_image": upload_file},
            format="multipart"
        )

        self.user.refresh_from_db()

        assert response.status_code == status.HTTP_200_OK
        mock_s3_delete.assert_called_once_with(old_image_name)  # Ensure old image is deleted
        assert self.user.profile_image.name != old_image_name  # New image is stored

    def test_upload_profile_image_invalid_data(self):
        """
        Test uploading an invalid profile image (wrong file type).
        """
        # Create an invalid file (text file instead of an image)
        invalid_file = SimpleUploadedFile("test.txt", b"invalid content", content_type="text/plain")

        response = self.client.post(
            "/api/users/upload-profile-image/",
            {"profile_image": invalid_file},
            format="multipart"
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "profile_image" in response.data

    def test_upload_profile_image_corrupt_data(self):
        """
        Test uploading an invalid profile image (corrupt image data).
        """
        # Create a fake corrupted image file
        corrupt_image = SimpleUploadedFile("corrupt.jpg", b"not-an-image", content_type="image/jpeg")

        response = self.client.post(
            "/api/users/upload-profile-image/",
            {"profile_image": corrupt_image},  
            format="multipart"
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "profile_image" in response.data  


    def teardown_method(self):
        """Ensure images are deleted from S3 after tests"""
        if self.user.profile_image:
            s3_storage = S3Boto3Storage()
            s3_storage.delete(self.user.profile_image.name)
            s3_storage.delete("profile_images/old_image.jpg")
            self.user.profile_image.delete()

@pytest.mark.django_db
class TestUserProfile:
    def setup_method(self):
        """Set up test client and users."""
        self.client = APIClient()
        self.user = User.objects.create(
            username="profile_user",
            email="profile@example.com",
            first_name="First",
            last_name="Last"
        )
        self.client.force_authenticate(user=self.user)

    def test_get_user_profile(self):
        """
        Test retrieving a user's profile.
        """
        # Call the API endpoint with correct URL path
        response = self.client.get(f"/api/users/{self.user.pk}/")  # Updated URL path
        
        # Assertions
        assert response.status_code == 200
        assert response.data["username"] == self.user.username
        assert response.data["email"] == self.user.email

    def test_get_nonexistent_user_profile(self):
        """
        Test trying to get a profile for a nonexistent user.
        """
        # Use a non-existent user ID
        non_existent_id = self.user.pk + 99
        
        # Call the API endpoint
        response = self.client.get(f"/api/users/{non_existent_id}/")  # Updated URL path
        
        # Assertions
        assert response.status_code == 404