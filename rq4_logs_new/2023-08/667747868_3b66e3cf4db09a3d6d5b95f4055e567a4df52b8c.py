from django import forms
from django.utils.translation import gettext_lazy as _

class TourSearchForm(forms.Form):
    query = forms.CharField(label=_('key search'), max_length=100, required=False)
    min_price = forms.DecimalField(label=_('Minimum Price'), min_value=0, required=False)
    max_price = forms.DecimalField(label=_('Maximum Price'), min_value=0, required=False)
    start_date = forms.DateField(label=_('Start day'), required=False)
    end_date = forms.DateField(label=_('End day'), required=False)
    location = forms.CharField(label=_('Address'), max_length=100, required=False)