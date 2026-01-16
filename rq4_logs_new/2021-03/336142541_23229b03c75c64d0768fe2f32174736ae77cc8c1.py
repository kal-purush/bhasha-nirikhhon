import sqlite3
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Length, EqualTo, ValidationError


class DeletePasswordForm(FlaskForm):
    password = PasswordField('password', validators=[DataRequired()])
    delete = SubmitField('Confirm')