# Import smtplib for the actual sending function
import smtplib
# Here are the email package modules we'll need
from email.mime.image import MIMEImage
from email.MIMEMultipart import MIMEMultipart
# The OS module in Python provides a way of using operating system dependent functionality
import os

#recipient == recipient's email address
#sender == my email address & password == password for sender account
recipient = 'sample_recipient@gmail.com'
sender = 'sample_sender@gmail.com'
password = 'sample_password'

subject = 'Send images'

try:
	# Create the container email message
	msg = MIMEMultipart()
	msg['Subject'] = subject

	# Specify file path
	directory = 'C:\Users\me\Desktop\Github\ImageGallery'
	# For each image in the folder
	for filename in os.listdir(directory):
		path = os.path.join(directory, filename)
		fp = open(path, 'rb')
		img = MIMEImage(fp.read())
		fp.close()
		msg.attach(img)
		 
	# Specify host & port for gmail
	smtpserver  = smtplib.SMTP("smtp.gmail.com",587)
	# Identify yourself to an SMTP server using EHLO
	smtpserver.ehlo()
	smtpserver.starttls()
	# Log in on an SMTP server that requires authentication
	smtpserver.login(sender, password)		 
	# Terminate the SMTP session and close the connection
	smtpserver.sendmail(sender, recipient, msg.as_string())
	smtpserver.quit()
	print "Successfully sent email"
except smtplib.SMTPException:
   print "Error: unable to send email"