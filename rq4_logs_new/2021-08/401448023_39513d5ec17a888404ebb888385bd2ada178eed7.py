from django import forms
from captcha.fields import CaptchaField

class ShortcodeForm(forms.Form):
    long_url = forms.CharField(label='', max_length=200)
    captcha = CaptchaField()