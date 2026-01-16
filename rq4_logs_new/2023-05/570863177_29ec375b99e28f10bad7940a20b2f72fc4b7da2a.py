import os
import configparser
from email.message import EmailMessage
import ssl
import smtplib

def send_email(what_to_send):
    config = configparser.ConfigParser()
    config.read('config.ini')

    email_sender = config.get('Email', 'username')
    email_password = config.get('Email', 'password')
    email_receiver = config.get('Email', 'receiver')

    subject = 'Python Error Handling'
    body = what_to_send

    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender,email_password)
        smtp.sendmail(email_sender,email_receiver,em.as_string())
send_email('Trying out function')