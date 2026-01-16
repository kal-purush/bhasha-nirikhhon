from flask_sqlalchemy import SQLAlchemy

from hasher import validate

db = SQLAlchemy()

def validate_credentials(user, password):
    """Validate credentials
    """
    try:
        passwordhash = User.query.filter_by(username=user).one().passwordhash
    except:
        return False

    validate(passwordhash, password)


class User(db.Model):
    '''DB model to store users' infromation'''
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True, unique=True)
    passwordhash = db.Column(db.String(120), index=True, unique=True)

    @property
    def is_authenticated(self):
        return True

    @property
    def is_active(self):
        return True

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return unicode(self.id) 

    def __repr__(self):
        return '<User %r>' % (self.username)