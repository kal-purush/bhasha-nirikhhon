from . import db


class UserProfile(db.Model):
    userid = db.Column(db.Integer, primary_key=True)
    fname = db.Column(db.String(80))
    lname = db.Column(db.String(80))
    gender = db.Column(db.String(80))    
    email = db.Column(db.String(80))  
    location = db.Column(db.String(255))    
    biography = db.Column(db.String(255))
    created_on = db.Column(db.String(255))    
    photo = db.Column(db.String(80))
    

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        try:
            return unicode(self.id)  # python 2 support
        except NameError:
            return str(self.id)  # python 3 support

    def __repr__(self):
        return '<User %r>' % (self.username)