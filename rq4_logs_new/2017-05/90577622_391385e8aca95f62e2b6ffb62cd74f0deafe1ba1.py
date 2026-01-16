#!/usr/bin/env python
# coding:utf8
from flask import Flask,request
from mailSender import mailSender

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/sendmail', methods=['POST'])
def sendmail():
    form = request.form
    subject=str(form['s'])
    text=str(form['t'])
    receivers=[str(x) for x in str(form['r']).split(',')]
    a = mailSender(subject,text,receivers)
    print type(receivers)
    print str('subject:%s,text:%s,receivers:%s' % (subject,text,str(receivers)))


    return str('%s %s %s' % (subject,text,str(receivers))) + '<br>' + str(a)


if __name__ == '__main__':
    app.run()