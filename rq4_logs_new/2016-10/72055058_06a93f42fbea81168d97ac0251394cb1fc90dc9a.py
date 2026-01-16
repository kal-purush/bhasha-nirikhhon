from django import forms

class LoginForm(forms.Form):
    username = forms.CharField(
        max_length=30,
        widget=forms.TextInput(attrs={'class': "input-lg col-md-4 col-md-offset-4"}),
    )

    password = forms.CharField(
        max_length=30,
        widget=forms.TextInput(attrs={'class': "input-lg col-md-4 col-md-offset-4"}),
    )
