# Sending Emails with Python using smtplib and email modules.

import smtplib
from email.message import EmailMessage

email = EmailMessage()
email['from'] = 'Ankur Bhalla'
email['to'] = '<ankurbhalla28@gmail.com>'
email['subject'] = 'You won 1,000,000 dollars!'

email.set_content('I am a Python Master!')

with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:
    # SMTP.ehlo(name=''). Identify yourself to an ESMTP server using EHLO.
    smtp.ehlo()

    # SMTP.starttls(keyfile=None, certfile=None, context=None)
    # Put the SMTP connection in TLS (Transport Layer Security) mode. All SMTP commands that follow
    # will be encrypted. You should then call ehlo() again.
    smtp.starttls()

    # SMTP.login(user, password, *, initial_response_ok=True)
    # Log in on an SMTP server that requires authentication. The arguments are the username and the
    # password to authenticate with.
    smtp.login('<enter sender email>', '<enter sender password>')

    # SMTP.send_message(msg, from_addr=None, to_addrs=None, mail_options=(), rcpt_options=()).
    # This is a convenience method for calling sendmail() with the message represented by an
    # email.message.Message object.
    smtp.send_message(email)

    print('all good boss!')