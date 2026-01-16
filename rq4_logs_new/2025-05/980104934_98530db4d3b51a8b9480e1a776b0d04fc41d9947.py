from django import forms
from core.models import Contact


class ContactForm(forms.ModelForm):

    class Meta:
        model = Contact
        fields = [
            'first_name',
            'last_name',
            'email',
            'message'
        ]
        widgets = {
            'first_name' : forms.TextInput(attrs={
                'class' : 'form-control',
                'placeholder' : 'Enter your firstname'
            }),
            'last_name' : forms.TextInput(attrs={
                'class' : 'form-control',
                'placeholder' : 'Enter your lastname'
            }),
            'email' : forms.EmailInput(attrs={
                'class' : 'form-control',
                'placeholder' : 'Enter your email'
            }),
            'message' : forms.Textarea(attrs={
                'class' : 'form-control',
                'cols' : 30,
                'rows' : 5,
                'placeholder' : 'Enter your message'
            })
        }