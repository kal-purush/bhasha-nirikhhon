from django import forms
from django.forms import ModelForm, Textarea
from myblog.models import *
from django.contrib.auth.models import User
from django.contrib.auth.forms import PasswordResetForm
class RegistrationForm(forms.Form):
    username = forms.CharField(max_length = 50,
                               widget=forms.TextInput(attrs={'placeholder': 'Username'}),)
    email1 = forms.EmailField(max_length = 100,
                             widget=forms.EmailInput(attrs={'placeholder': 'Email'}),)
    email2 = forms.EmailField(max_length = 100,
                             widget=forms.EmailInput(attrs={'placeholder': 'Confirm Email'}),)
    password1 = forms.CharField(max_length = 200,
                                widget = forms.PasswordInput(attrs={'placeholder': 'Password'}),)
    password2 = forms.CharField(max_length = 200,
                                widget = forms.PasswordInput(attrs={'placeholder': 'Confirm Password'}),)

    def clean(self):
        # Calls our parent (forms.Form) .clean function, gets a dictionary
        # of cleaned data as a result
        cleaned_data = super(RegistrationForm, self).clean()
        # Confirms that the two email fields match
        email1 = cleaned_data.get('email1')
        email2 = cleaned_data.get('email2')
        if email1 and email2 and email1 != email2:
            raise forms.ValidationError("Email did not match.")
        # Confirms that the two password fields match
        password1 = cleaned_data.get('password1')
        password2 = cleaned_data.get('password2')
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError("Passwords did not match.")


        # Generally return the cleaned data we got from our parent.
        return cleaned_data


    # Customizes form validation for the username field.
    def clean_username(self):
        # Confirms that the username is not already present in the
        # User model database.
        username = self.cleaned_data.get('username')
        if User.objects.filter(username__exact=username):
            raise forms.ValidationError("Username already exists.")

        # Generally return the cleaned data we got from the cleaned_data
        # dictionary
        return username

    def clean_email1(self):
        # Confirms that the username is not already present in the
        # User model database.
        email = self.cleaned_data.get('email1')
        if User.objects.filter(email__exact=email):
            raise forms.ValidationError("Email already exists.")

        # Generally return the cleaned data we got from the cleaned_data
        # dictionary
        return email



class MessageForm(forms.ModelForm):
    class Meta:
        model = Message
        fields = ('picture',)
        widgets = {
            'picture' : forms.FileInput(),
        }

class ProfileForm(forms.ModelForm):
    class Meta:
        model = Profile
        exclude = ('owner', )
        widgets = {
            'picture' : forms.FileInput(),

        }