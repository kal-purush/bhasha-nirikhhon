from django import forms
from django.forms import ModelForm
from .models import Cadet, Vehicle, Ride, Destination

class cadetForm(forms.ModelForm):
    class Meta:
        model = Cadet
        fields = '__all__'

class vehicleForm(forms.ModelForm):
    class Meta:
        model = Vehicle
        fields = '__all__'


class RideForm(forms.ModelForm):
    class Meta:
        model = Ride
        fields = '__all__'