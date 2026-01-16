from django.contrib.auth.models import User
from django import forms


class signup_form(forms.ModelForm):
    confirm_password = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'form-control',
                                                                         'id': 'confirm_password'}))

    class Meta:
        model = User
        fields = ['username', 'email', 'first_name', 'last_name', 'password']
        widgets = {
            'username': forms.TextInput(attrs={'class': 'form-control', 'id': 'username'}),
            'email': forms.EmailInput(attrs={'class': 'form-control', 'id': 'email'}),
            'first_name': forms.TextInput(attrs={'class': 'form-control', 'id': 'first_name'}),
            'last_name': forms.TextInput(attrs={'class': 'form-control', 'id': 'last_name'}),
            'password': forms.PasswordInput(attrs={'class': 'form-control', 'id': 'password'}),
        }