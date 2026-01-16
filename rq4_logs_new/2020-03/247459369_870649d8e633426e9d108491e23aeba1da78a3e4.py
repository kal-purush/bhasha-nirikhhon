from django import forms

class information(forms.Form):
    username = forms.CharField(label='Your name', max_length=100)