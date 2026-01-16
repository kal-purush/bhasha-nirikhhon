from django import forms
from .models import Resource, Meeting

class ResourceForm(forms.ModelForm):
	class Meta:

		# RESOURCE is the table we are gonna use for this form
		model=Resource
		fields='__all__'