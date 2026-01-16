from django import forms
from core.models import Images
class Upload(forms.ModelForm):
    content = forms.ImageField(required=True,label="",widget=forms.FileInput(attrs={'class':'form-control'}))
    class Meta:
        model = Images
        fields = ['content']
        widgets={
            "content" : forms.ImageField(attrs={'class':"form-control"})
        }