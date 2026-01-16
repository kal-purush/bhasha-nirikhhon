from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from flaskblog.models import Gallery
from wtforms.validators import DataRequired
from flask_uploads import UploadSet, IMAGES, configure_uploads


photos = UploadSet('photos', IMAGES)


class GalleryForm(FlaskForm):
    filepath = StringField('Filepath', validators=[DataRequired()])
    location = StringField('Location', validators=[DataRequired()])
    caption = StringField('Caption', validators=[DataRequired()])
    submit = SubmitField('Upload to Gallery')