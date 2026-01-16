import smtplib, ssl, sys

#from file import *

##try: notif
##except NameError: notif = None

##if notif is None:
##    sys.exit()
##message = notif

smtp_server = "smtp.gmail.com"
port = 587  # For SSL
sender_email = "wifiologyproject@gmail.com"
password = "Wifi1234-"
receiver_email = "wifiologyproject@gmail.com"
message = """\
Subject: Hi there

This message is sent from Python."""

# Create a secure SSL context
context = ssl.create_default_context()
with smtplib.SMTP(smtp_server, port) as server:
    server.ehlo()  # Can be omitted
    server.starttls()
    server.ehlo()  # Can be omitted
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)