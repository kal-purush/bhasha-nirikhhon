from wtforms import Form, TextAreaField, validators, StringField

class AssignProject(Form):
    tittle = StringField('Tittle', [validators.length(min=1, max=200)])
    body = TextAreaField('Body', [validators.length(min=30)])
    name = StringField('Name:', [validators.length(min=1, max=50)])
    email = StringField('Email:', [validators.length(min=6, max=50)])
    expected_date = StringField('End at:', [validators.DataRequired()])

