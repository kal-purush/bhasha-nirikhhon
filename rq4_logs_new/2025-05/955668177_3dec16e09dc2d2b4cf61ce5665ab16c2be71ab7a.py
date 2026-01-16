from flask import Flask, render_template, request
from collections import defaultdict
import os

app = Flask(__name__)

@app.route('/')
def index() :
    return render_template('index.html')

@app.route('/input')
def input() :
    return render_template('input.html')

dic = defaultdict(lambda:'images/뚱.webp')

dic['조바이든'] = 'images/조바이든.webp'
dic['172'] = 'images/172.webp'
dic['하니'] = 'images/하니.webp'
dic['윤'] = 'images/백지헌.webp'

@app.route('/result',methods=['POST'])
def result():
    profile_pics  = request.form.getlist('profile_pic[]')
    names = request.form.getlist('name[]')
    role = request.form.getlist('Role[]')
    student_numbers = request.form.getlist('StudentNumber[]')
    department = request.form.getlist('Department[]')
    emails = request.form.getlist("email[]")
    genders = request.form.getlist('gender[]')
    foods   = request.form.getlist('favorite_food[]')
    Languages = request.form.getlist('Languages[]')
    dreams = request.form.getlist('Dream[]')
    phonenumber = request.form.getlist('Phonenumber[]')
    MBTI = request.form.getlist('MBTI[]')
    return render_template('result.html',
                          students=zip(
                            names, role, student_numbers, department,
                            phonenumber, emails, genders, foods,
                            MBTI, dreams, Languages, profile_pics
                           ))    

@app.route('/contact')
def contact_info():
    return render_template('contact.html')