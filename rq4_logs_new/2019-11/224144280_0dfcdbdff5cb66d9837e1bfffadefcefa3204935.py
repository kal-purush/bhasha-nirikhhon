import smtplib
from email.message import EmailMessage
from string import Template
from pathlib import Path

html = Template(Path('index.html').read_text()) # access template file and read it as a string
email = EmailMessage() # add email object
email['from'] = 'Caroline Chan'# add email from key
email['to'] = 'hello@caroline-chan.com' # add email to key
email['subject'] = 'You won!' #add subject line

email.set_content(html.substitute({'name':'TinTin'}), 'html') # set the email content with name variable

with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp: # set the server and host
    smtp.ehlo() # command sent by an email server to identify itself
    # when connecting to another email server to start the process of sending an email.
    smtp.starttls() # encryption to connect securely to the server
    smtp.login('pythontest11111@gmail.com', 'ptesting1') #login to the server
    smtp.send_message(email) # send the email
    print('all good') # print if no errors