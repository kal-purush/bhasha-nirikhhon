from django import forms
from .models import Ordenes

class OrdenesForm(forms.ModelForm):
    class Meta:
        model = Ordenes
        fields = ['id', 'nombreCliente', 'total']