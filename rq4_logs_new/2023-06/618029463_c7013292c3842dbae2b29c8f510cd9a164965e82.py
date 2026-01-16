""" Libor Havránek App Copyright (C)  2.6 2023 """


from flask_wtf import FlaskForm
from wtforms import StringField, FloatField, SelectField, HiddenField
from wtforms.validators import DataRequired, Email


class CustomerOrderForm(FlaskForm):
    customer_first_name = StringField('Jméno:', validators=[DataRequired()])
    customer_last_name = StringField('Příjmení:', validators=[DataRequired()])
    customer_email = StringField('Email:', validators=[DataRequired(), Email()])
    customer_phone_code = SelectField('Předčíslí:', choices=[('+420', '+420'), ('+421', '+421')])
    customer_phone = StringField('Telefon:', validators=[DataRequired()])
    customer_city = StringField('Město:', validators=[DataRequired()])
    customer_street = StringField('Ulice:', validators=[DataRequired()])
    customer_zipcode = StringField('PSČ:', validators=[DataRequired()])
    customer_info = StringField('Info(např. patro):')


