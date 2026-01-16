from datetime import datetime, timedelta
import uuid

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.core.exceptions import ValidationError
from django.db import models
from core.mixins import TimeStampMixin


class UserManager(BaseUserManager):
    def create_user(self, first_name, last_name, email, password=None):
        """
        Create and return a `User` with:
            first name, last name, email and password.
        """
        if first_name is None:
            raise TypeError('Users must have a first name.')

        if last_name is None:
            raise TypeError('Users must have a last name.')

        if email is None:
            raise TypeError('Users must have an email address.')

        user = self.model(
            first_name=first_name,
            last_name=last_name,
            email=self.normalize_email(email)
        )
        user.set_password(password)
        user.save()

        return user

    def create_superuser(self, first_name, last_name, email, password):
        """
        Create and return a `User` with superuser (admin) permissions.
        """
        if password is None:
            raise TypeError('Superusers must have a password.')

        user = self.create_user(email, password)
        user.is_superuser = True
        user.is_staff = True
        user.save()

        return user


class User(AbstractBaseUser, PermissionsMixin, TimeStampMixin):
    """
    User object for all Application users
    Implements a UUID field
    """
    class Meta:
        db_table = 'user'

    id = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    first_name = models.CharField(max_length=50)
    last_name = models.CharField(max_length=50)
    email = models.EmailField(db_index=True, unique=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    USERNAME_FIELD = 'email'
    objects = UserManager()

    def __str__(self):
        return self.email

    @property
    def get_full_name(self):
        return f'{self.first_name} {self.last_name}'

    def get_short_name(self):
        return f'{self.first_name}'
    
    def __str__(self):
        return f'{self.first_name}-{self.email}'