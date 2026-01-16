from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Submit, Layout, Fieldset, HTML, Field
from crispy_forms.bootstrap import (
    PrependedText,
    PrependedAppendedText,
    FormActions
)

class ContactUsForm(forms.Form):
    subject = forms.CharField(max_length=100, label='Тема письма', required=False)
    message = forms.CharField(widget=forms.Textarea, label='Сообщение',required=False)
    first_name = forms.CharField(max_length=100, label='Имя', required=False)
    sender = forms.EmailField(label = 'Ваша почта')
    cc_myself = forms.BooleanField(label = 'Отправить копию письма на Ваш адрес?', required=False)
    def __init__(self, *args, **kwargs):
        super(ContactUsForm, self).__init__(*args, **kwargs)
        self.helper = FormHelper(self)
        self.helper.form_method = 'POST'
        self.helper.layout = Layout(Fieldset(
        '',
        'subject',
        'message',
        'first_name',
        'sender',
        'cc_myself',
        HTML("{% csrf_token %}"),
        FormActions(Submit('purchase', 'ОТПРАВИТЬ', css_class='btn-primary btn-block'))
        ))