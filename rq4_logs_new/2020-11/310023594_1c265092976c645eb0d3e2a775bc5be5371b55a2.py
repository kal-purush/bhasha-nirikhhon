
from django.contrib.auth.models import User
from userManagementSystem.models import UserProfileInfo
from django import forms

# Create your models here.
class userRegistrationForm(forms.ModelForm):

    password = forms.CharField(label='Password',widget= forms.TextInput(attrs={'class': 'form-control'}))
    password2 = forms.CharField(label='Repeat Password', widget=forms.PasswordInput)

    class Meta:
        model = User
        fields = ('first_name','last_name','username')


    def clean_password2(self):
        cd = self.cleaned_data
        if cd['password'] != cd['password2']:
            raise forms.ValidationError('Passwords don\'t match.')
        return cd['password2']


class UserProfileInfoForm(forms.ModelForm):
    
    class Meta:
        model = UserProfileInfo
        fields = ('userType','profilePic')

        
  