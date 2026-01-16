from flask import render_template, Flask
from flask_mail import Message, Mail

app = Flask(__name__)


def send_reset_password_email(accessCode, email):
    msg = Message("Welcome",
                  sender='itersysecommerce@gmail.com',
                  recipients=['rfigueroa@intersysconsulting.com'
                              ])  # Should be changed to customer's email
    with app.open_resource("../../../templates/logo.jpg") as fp:
        msg.attach('logo.jpg',
                   'image/jpg',
                   fp.read(),
                   'inline',
                   headers=[
                       ['Content-ID', '<Myimage>'],
                   ])
        msg.html = render_template('email.html', code=accessCode, email=email)
    mail = Mail()
    mail.send(msg)