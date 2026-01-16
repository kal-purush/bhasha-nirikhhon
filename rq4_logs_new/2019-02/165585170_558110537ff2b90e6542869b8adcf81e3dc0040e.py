from django.core.mail import send_mail, EmailMessage, get_connection
from django.conf import settings
from django.template.loader import get_template

def email(subject, recipient, template_path, context):
    """
    Sends emails to users
    """
    connection = get_connection()
    template = get_template(template_path)
    message_content = template.render(context)
    sender = settings.EMAIL_HOST_USER
    message = EmailMessage(subject, message_content, to=recipient, from_email=sender)
    message.content_subtype = 'html'
    message.send()
    connection.close()