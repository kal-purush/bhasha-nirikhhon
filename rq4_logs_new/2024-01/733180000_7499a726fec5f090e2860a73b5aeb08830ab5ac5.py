from django import forms
from django.utils.translation import gettext_lazy as _

from core.User.models import User
from core.Utils.fields import PasswordField


class ProfileForm(forms.ModelForm):
    class Meta:
        model = User
        fields = (
            'first_name',
            'last_name',
        )


class ProfileChangePasswordForm(forms.Form):
    current_password = PasswordField(label=_('Current password'), add_validators=False)
    password = PasswordField(label=_('Password'))
    confirm_password = PasswordField(label=_('Confirm password'))

    def __init__(self, *args, **kwargs):
        self.instance = kwargs.pop('instance')
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned_data = super().clean()

        instance = self.instance
        if not instance.check_password(cleaned_data.get('current_password')):
            msg = _('Current password is not correct')
            self.add_error('current_password', msg)

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