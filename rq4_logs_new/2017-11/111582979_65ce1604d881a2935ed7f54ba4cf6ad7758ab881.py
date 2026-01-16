from django import forms
from .models import Materia

class MateriaForm(forms.ModelForm):

    class Meta:
        model = Materia
        fields = ('nombre','profesor','nota')