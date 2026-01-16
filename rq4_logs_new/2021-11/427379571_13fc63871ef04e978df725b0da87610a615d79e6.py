from flask import Flask, render_template, request, redirect
from flask_wtf import FlaskForm
from wtforms import SubmitField, StringField

app = Flask(__name__)
app.config['SECRET_KEY'] = '4a7139823b8f9f3f4d5512f7b0b1b9add4cc843c7b4506cba7189305dfe55158'