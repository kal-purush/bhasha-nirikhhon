from django import forms
from django.core.exceptions import ValidationError
from django.contrib.auth import get_user_model, authenticate
from django.contrib.auth.password_validation import validate_password

User = get_user_model()


class LoginForm(forms.Form):
    username = forms.CharField(widget=forms.TextInput(attrs={'class': 'custom-selector'}))
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'custom-selector'}))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = None

    def clean(self):
        cd = super().clean()
        username = cd.get('username')
        password = cd.get('password')
        self.user = authenticate(username=username, password=password)
        if self.user is None:
            raise ValidationError('Podaj poprawne dane!')


class UserAddForm(forms.ModelForm):
    password1 = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'custom-selector'}))
    password2 = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'custom-selector'}))

    class Meta:
        model = User
        fields = (
            'username',
            'email',
        )

        help_texts = {
            'username': ""
        }

        widgets = {
            'username': forms.TextInput(attrs={'class': 'custom-selector'}),
            'email': forms.TextInput(attrs={'class': 'custom-selector'}),
        }

    def clean(self):
        cd = super().clean()
        pass1 = cd.get('password1')
        pass2 = cd.get('password2')
        validate_password(pass1)
        if pass1 != pass2:
            raise ValidationError('Password must be the same')
