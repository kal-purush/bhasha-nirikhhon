from .models import Category, Gems
from django import forms

class GemsForm(forms.ModelForm):
    class Meta:
        model = Gems
        fields =['title', 'description', 'balance', 'category']