from app import db
#import app

class Metadata(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    mean = db.Column(db.Float)
    std = db.Column(db.Float)#nullable=Flase
    blarg = db.Column(db.Float)
    level = db.Column(db.String(1))
    #fpath = db.Column(db.String(80), nullable=False)???

    def __repr__(self):
        return '<id %r>' % (self.id)

    def to_dict(self):
        return dict(id=self.id, 
                    blarg=self.blarg, 
                    level=self.level, 
                    mean=self.mean, 
                    std=self.std)#, 
                    #fpath=self.fpath)