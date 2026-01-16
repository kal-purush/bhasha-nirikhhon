from django import forms
from . import models

class SelectPreferences(forms.ModelForm):
    class Meta:
        model = models.UserProfile
        fields = ['user', 'choices']