from django import forms
from django.forms import ModelForm
from .models import User


class UserForm(ModelForm):
    class Meta:
        model = User
        fields = ['name']

    def create_user(self):
        print("Running this method")
        user = User()
        user.name = self.cleaned_data['name']
        user.save()
        return user