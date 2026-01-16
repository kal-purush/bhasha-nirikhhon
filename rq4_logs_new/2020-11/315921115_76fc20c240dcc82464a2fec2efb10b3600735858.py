from django import forms

from evaluate.models import Evaluate


class EvaluateForm(forms.ModelForm):
    class Meta:
        model = Evaluate
        fields = ['time', 'cafeteria', 'menu', 'contents']

        widgets = {
            'time': forms.TextInput(attrs={'class': 'form-control'}),
            'cafeteria': forms.TextInput(attrs={'class': 'form-control'}),
            'menu': forms.TextInput(attrs={'class': 'form-control'}),
            'contents': forms.Textarea(attrs={'class': 'form-control', 'rows': 5}),
        }