from django import forms
from django.forms import ModelForm
from DIY.models import *



class DIYUserForm(ModleForm):
    #username = forms.CharField(label='Desired username', max_length=30)
    #first_name = forms.CharField(label='Your first name', max_length=30)
    #last_name = forms.CharField(label='Your last name', max_length=30)
    #email = forms.EmailField()
    #password = forms.Pass
    class Meta:
        model = DIYUser
        fields = ['username','first_name','last_name','email','password']

    

class ItemServiceForm(ModelForm):
    class Meta:
        model = ItemService
        fields = ['image','details',]

class OfferForm(forms.Form):
    pass
