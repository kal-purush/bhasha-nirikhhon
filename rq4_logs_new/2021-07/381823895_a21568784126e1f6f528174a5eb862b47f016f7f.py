from django.db import models


class UserAdmin(models.Model):
    admin_id = models.BigAutoField(blank=False, primary_key=True)
    name = models.CharField(max_length=40, blank=False, null=False)
    email = models.EmailField(max_length=254, blank=False)
    password = models.CharField(max_length=50, blank=False)