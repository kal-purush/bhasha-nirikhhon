from django import forms
from django.forms.widgets import CheckboxInput

class TestForm(forms.Form):
    username = forms.CharField(
        max_length=50,
        label="Username", 
        error_messages={
            "required":"Username in required",
            "max_length":"Put shorter name",},
    )
    password = forms.CharField(
        max_length=16,
        min_length=8,
        label="Password", 
        error_messages={
            "required":"Password in required",
            "max_length":"Put shorter password",
            "min_length":"Put longer password"},
        widget=forms.PasswordInput
    )
    
    remember = forms.BooleanField(
        widget=CheckboxInput,
        label="Remember Me",
        required=False,
    )