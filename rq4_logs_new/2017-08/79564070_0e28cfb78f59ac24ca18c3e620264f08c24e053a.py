import logging
import os

import requests
from django.template.loader import get_template


class MailgunHandler(logging.Handler):
    def emit(self, record):
        dest = os.getenv('MAIL_DEST', None)
        mail_template = get_template('board/mail/log.html')
        mail_html = mail_template.render({'record': record})

        subject = 'Log {}'.format(record.levelname)
        send_mail(subject, dest, mail_html)


def send_mail(subject, dest, mail_html):
    domain = os.getenv('MAILGUN_DOMAIN')
    api_key = os.getenv('MAILGUN_API_KEY')

    requests.post(
        "https://api.mailgun.net/v3/{0}/messages".format(domain),
        auth=("api", api_key),
        data={
            "from": "Dashboard <noreply@mg.bde-insa-lyon.fr>",
            "to": dest.split(','),
            "subject": subject,
            "html": mail_html,
            "o:tracking-clicks": "no",
            "o:tracking-opens": "no",
            "o:tracking": "no"
        }
    )