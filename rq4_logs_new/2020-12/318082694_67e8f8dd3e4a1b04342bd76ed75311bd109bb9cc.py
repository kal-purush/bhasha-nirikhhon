from django.db import models
from django.contrib.auth.models import AbstractUser as user_parent

# Create your models here.

class Users(user_parent):
    class GenderCtg(models.IntegerChoices):
        COMMON = 0
        WOMAN = 1
        MAN = 2
    Gender = models.IntegerField(choices=GenderCtg.choices)
    Birth = models.DateField()
    Contact = models.CharField(max_length=25)
    Name = models.CharField(max_length=20)

    class Meta(user_parent.Meta):
        swappable = 'AUTH_USER_MODEL'