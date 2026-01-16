from django import forms
from .models import Publicar

class postearForms(forms.ModelForm):

    class Meta:
        model=Publicar
        fields=('titulo','texto',)