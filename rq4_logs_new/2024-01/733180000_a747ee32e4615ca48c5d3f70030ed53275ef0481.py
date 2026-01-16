from django import forms


class LoginForm(forms.Form):
    email = forms.EmailField(max_length=254, required=True,
                             widget=forms.EmailInput(attrs={'autofocus': True}))
    password = forms.CharField(label='Password', strip=True,
                               min_length=8, max_length=128,
                               widget=forms.PasswordInput(attrs={'autocomplete': 'current-password'}))
    remember_me = forms.BooleanField(label='Remember Me', required=False)

    def clean_password(self):
        password = self.cleaned_data.get('password')
        if not password:
            raise forms.ValidationError('This field is required.')
        return password