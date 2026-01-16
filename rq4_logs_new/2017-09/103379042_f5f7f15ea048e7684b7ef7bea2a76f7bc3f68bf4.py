#coding:utf-8

from flask import Flask,Markup,url_for
import os
app = Flask(__name__)
# with app.test_request_context():
#     print url_for('index')

@app.route('/') #index
def index():
    user_agent=request.headers.get('User-Agent')
    return 'Index Page'

@app.route('/login')  # 登录
def login():
    return 'please login'

@app.route('/reg') #注册
def reg():
    return 'reg new user'

@app.route('/admin') #管理员
def manager():
    return 'Welcome admin!'



if __name__ == '__main__':
    app.run()