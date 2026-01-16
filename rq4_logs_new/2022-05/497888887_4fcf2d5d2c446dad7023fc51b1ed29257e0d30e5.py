import django
from django.db import models
from django.contrib.auth.models import (
    BaseUserManager, AbstractBaseUser
)

class UserManager(BaseUserManager):
    def create_user(self, email, user_type, password=None):   
        if not email:
            raise ValueError('Users must have an email address')
        user = self.model(
            email= self.normalize_email(email),
            user_type= user_type,   
        )
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, email, password):   
        user = self.model(
            email= email,
            user_type= "Super admin",
            password=password   
        )
        user.save(using=self._db)
        return user

# Create your models here.
class User(AbstractBaseUser):
    email = models.EmailField(
        verbose_name='email address',
        max_length=255,
        unique=True,
        blank=True, 
        null=True
    ) 
    user_type= models.CharField(max_length=255, blank=True, null=True)
    objects= UserManager

    def __str__(self):
        return self.email

    def has_perm(self, perm, obj=None):
        return True

    def has_module_perms(self, app_label):
        return True
    
    @property
    def is_staff(self):
        return self.user_type == "Super admin"