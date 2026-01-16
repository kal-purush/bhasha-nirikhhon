# @Time: 2022/9/19 16:48
# @Author: 李树斌
# @File : Mail.py
# @Software :PyCharm
from flask import Flask, render_template
from flask_mail import Message
from flask_mail import Mail
import os
app = Flask(__name__)
app.config=['FLASKY_MAIL_SUBJECT_PREFIX'] = '[FLASKY]'
app.config=['FLASKY_MAIL_SENDER'] = 'Flasky Admin <2697601945@qq.com>'
mail = Mail(app)
app.config['MAIL_SERVER'] = 'smtp.qq.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USER_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')

def send_email(to,subject,template,**kwargs):
    msg = Message(app.config['FLASKY_MAIL_SUBJECT_PREFIX'] + subject,
                  sender=app.config['FLASKY_MAIL_SENDER'],recipients=[to])
    msg.body = render_template(template +'.txt',**kwargs)
    msg.html = render_template(template + '.html',**kwargs)
    mail.send(msg)


















if __name__ == '__main__':
    app.run()