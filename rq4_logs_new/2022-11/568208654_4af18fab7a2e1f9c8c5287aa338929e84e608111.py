from django.forms import ModelForm
from .models import Cliente

class ClienteForm(ModelForm):
    class Meta:
        model= Cliente
        fields= ["nome", "cpf_cnpj", "tipo"]

class Form(forms, Form):
    nome = forms.CharField(max_length=100, min_length=3)