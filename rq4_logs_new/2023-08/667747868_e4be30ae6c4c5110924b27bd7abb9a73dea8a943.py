from django.core.mail import BadHeaderError, send_mail
from django.http import HttpResponse
from django.core.mail import EmailMultiAlternatives
from booking import settings
from django.template.loader import render_to_string


def send_mail_custom(subject, to, text_content, template, **kwargs):
    try:
        text_content = "This is an important message."
        msg_html = render_to_string(template, {"params": kwargs})
        msg = EmailMultiAlternatives(
            subject, text_content, settings.DEFAULT_FROM_EMAIL, [to]
        )
        msg.attach_alternative(msg_html, "text/html")
        msg.send()
    except BadHeaderError:
        return HttpResponse("Invalid header found.")