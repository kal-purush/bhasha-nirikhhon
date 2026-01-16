from django import forms

from advanced_tactics.models import UserProfile


class UserProfileForm(forms.ModelForm):
    class Meta:
        widgets = {
            'birthdate': forms.DateInput(attrs={'class':'datepicker'}),
        }
        model = UserProfile
        fields = ['user', 'favorite_hobby', 'hair_color', 'birthdate', 'created', 'phone', 'city', 'state', 'zipcode'] # only show these fields
        # fields = '__all__' # default behavior
        # exclude = ['birthdate', 'favorite_hobby',] # don't show these fields