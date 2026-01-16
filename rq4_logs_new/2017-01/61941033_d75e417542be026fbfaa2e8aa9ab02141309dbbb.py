from django.contrib.auth.models import User
from django import forms
from .models import Note,ListDo,ListContent
from django.forms.models import inlineformset_factory
class UserForm(forms.ModelForm):
	password =forms.CharField(widget=forms.PasswordInput)

	class Meta:
		model = User
		fields =['username','password']

class NoteForm(forms.ModelForm):

	class Meta:
		model = Note
		fields =['note_title','note_content','note_remainder']
class ListDoForm(forms.ModelForm):

	class Meta:
		model = ListDo
		fields =['listdo_title','listdo_remainder']

class ListContentForm(forms.ModelForm):

	class Meta:
		model = ListContent
		fields =['content','is_checked']
	
ListContentFormSet = inlineformset_factory(ListDo, ListContent,exclude=['listdo',],)
