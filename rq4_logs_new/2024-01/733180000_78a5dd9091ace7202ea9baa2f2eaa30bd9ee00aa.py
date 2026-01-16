from django import forms
from django.utils.translation import gettext_lazy as _

from core.User.models import User
from core.Utils.Filters.filtersets import BaseFilterForm
from core.Utils.fields import PasswordField


class AdminsFilterForm(BaseFilterForm):
    class Meta(BaseFilterForm.Meta):
        model = User
        search_fields = ('email',)
        activity_field = 'is_active'


class AdminBaseForm(forms.ModelForm):
    is_active = forms.BooleanField(label=_('Is active'), required=False,
                                   widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}))
    is_staff = forms.BooleanField(label=_('Is staff'), required=False,
                                  widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}))
    is_superuser = forms.BooleanField(label=_('Is superuser'), required=False,
                                      widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}))

    class Meta:
        model = User
        fields = (
            'email',
            'first_name',
            'last_name',
            'is_active',
            'is_staff',
            'is_superuser',
        )


class AdminAddForm(AdminBaseForm):
    pass


class AdminEditForm(AdminBaseForm):
    pass


class AdminSetPasswordForm(forms.Form):
    password = PasswordField(label=_('Password'))
    confirm_password = PasswordField(label=_('Confirm password'))

    def __init__(self, *args, **kwargs):
        self.instance = kwargs.pop('instance')
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data.get('password') != cleaned_data.get('confirm_password'):
            msg = _('Passwords do not match')
            self.add_error('password', msg)
            self.add_error('confirm_password', msg)
        return cleaned_data

    def save(self, **kwargs):
        instance = self.instance
        instance.set_password(self.cleaned_data['password'])
        instance.save()
        return instance