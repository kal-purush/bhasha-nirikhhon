from flask_wtf import FlaskForm, CSRFProtect
from wtforms import StringField, TextAreaField, SubmitField
from wtforms.validators import DataRequired, Email

csrf = CSRFProtect()

class ContactForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired('Name can not be empty')])
    email = StringField(
        'Email', 
        validators=[
            DataRequired('Email can not be empty'),
            Email('Please enter a valid email address')
        ]
    )
    subject = StringField('Subject', validators=[DataRequired('Subject can not be empty')])
    message = TextAreaField('Message', validators=[DataRequired('Message can not be empty')])
    submit = SubmitField("Send")