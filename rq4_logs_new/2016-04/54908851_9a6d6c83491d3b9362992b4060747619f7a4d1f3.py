# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 10:12:03 2016

@author: Cody
"""

from flask import Flask
from flask import request
from flask import render_template

app = Flask(__name__)


@app.route('/')
def my_form():
    return render_template("my-form.html")
	
@app.route('/choose')
def my_choose():
    return render_template("choose.html")
	
@app.route('/hunger')
def my_hunger():
    return render_template("hunger.html")
	
@app.route('/bored')
def my_bored():
    return render_template("bored.html")

@app.route('/results')
def my_results():
    return render_template("results.html")
	
@app.route('/results', methods=['POST'])
def my_form_post():
#take previously obtained inputs, send them through hopscotch, RETURN answer
    text = request.form['text1']
    processed_text = text.upper()
    return processed_text

if __name__ == '__main__':
    app.run()